# AutoMQ automq-metrics Module

##├── exporter/
│   ├── MetricsExporter.java       # Exporter interface
│   ├── MetricsExporterURI.java    # URI parser
│   ├── OTLPMetricsExporter.java   # OTLP exporter implementation
│   ├── PrometheusMetricsExporter.java # Prometheus exporter implementation
│   │   ├── PromConsts.java        # Prometheus constants
│   │   ├── PromLabels.java        # Label management for Prometheus format
│   │   ├── PromTimeSeries.java    # Time series data structures
│   │   ├── PromUtils.java         # Prometheus utility functions
│   │   ├── RemoteWriteExporter.java # Main remote write exporter
│   │   ├── RemoteWriteMetricsExporter.java # Metrics exporter adapter
│   │   ├── RemoteWriteRequestMarshaller.java # Request marshalling
│   │   ├── RemoteWriteURI.java    # URI parsing for remote write
│   │   └── auth/                  # Authentication support
│   │       ├── AuthType.java      # Authentication type enum
│   │       ├── AuthUtils.java     # Authentication utilities
│   │       ├── AwsSigV4Auth.java  # AWS SigV4 authentication
│   │       ├── AwsSigV4Interceptor.java
│   │       ├── AwsSigV4Signer.java
│   │       ├── AzureADAuth.java   # Azure AD authentication
│   │       ├── AzureADInterceptor.java
│   │       ├── AzureCloudConst.java
│   │       ├── BasicAuth.java     # HTTP Basic authentication
│   │   │   ├── BasicAuthInterceptor.java
│   │       ├── BearerAuthInterceptor.java # Bearer token authentication
│   │       ├── BearerTokenAuth.java
│   │       └── RemoteWriteAuth.java # Authentication interface
│   └── s3/                        # S3 metrics exporter implementationiew

The AutoMQ OpenTelemetry module is a telemetry data collection and export component based on OpenTelemetry SDK, specifically designed for AutoMQ Kafka. This module provides unified telemetry data management capabilities, supporting the collection of JVM metrics, JMX metrics, and Yammer metrics, and can export data to Prometheus, OTLP-compatible backend systems, or S3-compatible storage.

## Core Features

### 1. Metrics Collection
- **JVM Metrics**: Automatically collect JVM runtime metrics including CPU, memory pools, garbage collection, threads, etc.
- **JMX Metrics**: Define and collect JMX Bean metrics through configuration files
- **Yammer Metrics**: Bridge existing Kafka Yammer metrics system to OpenTelemetry

### 2. Multiple Exporter Support
- **Prometheus**: Expose metrics in Prometheus format through HTTP server
- **OTLP**: Support both gRPC and HTTP/Protobuf protocols for exporting to OTLP backends
- **S3**: Export metrics to S3-compatible object storage systems

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
│   ���── OTLPMetricsExporter.java   # OTLP exporter implementation
│   ├── PrometheusMetricsExporter.java # Prometheus exporter implementation
│   └── s3/                        # S3 metrics exporter implementation
│       ├── CompressionUtils.java  # Utility for data compression
│       ├── PrometheusUtils.java   # Utilities for Prometheus format
│       ├── S3MetricsConfig.java   # Configuration interface
│       ├── S3MetricsExporter.java # S3 metrics exporter implementation
│       ├── S3MetricsExporterAdapter.java # Adapter to handle S3 metrics export
│       ├── UploaderNodeSelector.java # Interface for node selection logic
│       └── UploaderNodeSelectors.java # Factory for node selector implementations
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

#### S3 Metrics Exporter
```properties
# S3 metrics exporter configuration
automq.telemetry.exporter.uri=s3://access-key:secret-key@my-bucket.s3.amazonaws.com
automq.telemetry.exporter.interval.ms=60000
automq.telemetry.s3.cluster.id=cluster-1
automq.telemetry.s3.node.id=1
automq.telemetry.s3.primary.node=true
```

Example usage with S3 exporter:

```java
// Create configuration for S3 metrics export
Properties props = new Properties();
props.setProperty("automq.telemetry.exporter.uri", "s3://access-key:secret-key@my-bucket.s3.amazonaws.com");
props.setProperty("automq.telemetry.s3.cluster.id", "my-kafka-cluster");
props.setProperty("automq.telemetry.s3.node.id", "1");
props.setProperty("automq.telemetry.s3.primary.node", "true");  // Only one node should be set to true
props.setProperty("service.name", "automq-kafka");
props.setProperty("service.instance.id", "broker-1");

// Initialize telemetry manager with S3 export
AutoMQTelemetryManager telemetryManager = new AutoMQTelemetryManager(props);
telemetryManager.init();

// Application running...

// Shutdown telemetry system
telemetryManager.shutdown();
```

### S3 Metrics Exporter Configuration

The S3 Metrics Exporter allows you to export metrics data to S3-compatible storage systems, with support for different node selection strategies to ensure only one node uploads metrics data in a cluster environment.

#### URI Format

```
s3://<access-key>:<secret-key>@<bucket-name>?endpoint=<endpoint>&<other-parameters>
```

S3 bucket URI format description:
```
s3://<bucket-name>?region=<region>[&endpoint=<endpoint>][&pathStyle=<enablePathStyle>][&authType=<authType>][&accessKey=<accessKey>][&secretKey=<secretKey>][&checksumAlgorithm=<checksumAlgorithm>]
```

- **pathStyle**: `true|false`. Object storage access path style. Set to true when using MinIO.
- **authType**: `instance|static`. When set to instance, instance profile is used for authentication. When set to static, accessKey and secretKey are obtained from the URL or system environment variables KAFKA_S3_ACCESS_KEY/KAFKA_S3_SECRET_KEY.

Simplified format is also supported, with credentials in the user info part:
```
s3://<access-key>:<secret-key>@<bucket-name>?endpoint=<endpoint>&<other-parameters>
```

Examples:
- `s3://accessKey:secretKey@metrics-bucket?endpoint=https://s3.amazonaws.com`
- `s3://metrics-bucket?region=us-west-2&authType=instance`

#### Configuration Properties

| Configuration | Description | Default Value |
|---------------|-------------|---------------|
| `automq.telemetry.s3.cluster.id` | Cluster identifier | `automq-cluster` |
| `automq.telemetry.s3.node.id` | Node identifier | `0` |
| `automq.telemetry.s3.primary.node` | Whether this node is the primary uploader | `false` |
| `automq.telemetry.s3.selector.type` | Node selection strategy type | `controller` |
| `automq.telemetry.s3.bucket` | S3 bucket URI | None |

#### Node Selection Strategies

In a multi-node cluster, typically only one node should upload metrics to S3 to avoid duplication. The S3 Metrics Exporter provides several built-in node selection strategies through the `UploaderNodeSelector` interface:

1. **Controller Runtime Leadership** (`controller`)

   Defers to the Kafka KRaft controller's leadership. When the AutoMQ broker runtime registers its supplier the exporter will automatically start uploading from the active controller node—no extra configuration required.

2. **Kafka Connect Runtime Leadership** (`connect-leader`)

   Uses the Kafka Connect distributed herder leadership. AutoMQ's Connect runtime registers the supplier once the worker joins the cluster, ensuring only the elected leader uploads metrics.

> **Note**
>
> Runtime-backed selectors (`controller`, `connect-leader`) poll the registry on every decision. Once the hosting runtime publishes or updates its supplier, the exporter immediately reflects the new leadership without restarting the telemetry pipeline.

1. **Custom SPI-based Selectors**

   The system supports custom node selection strategies through Java's ServiceLoader SPI mechanism.

   ```properties
   automq.telemetry.s3.selector.type=custom-type-name
   # Additional custom parameters as needed
   ```

#### Custom Node Selection using SPI

You can implement custom node selection strategies by implementing the `UploaderNodeSelectorProvider` interface and registering it using Java's ServiceLoader mechanism:

1. **Implement the Provider Interface**

   ```java
   public class CustomSelectorProvider implements UploaderNodeSelectorProvider {
       @Override
       public String getType() {
           return "custom-type"; // The selector type to use in configuration
       }
       
       @Override
       public UploaderNodeSelector createSelector(String clusterId, int nodeId, Map<String, String> config) {
           // Create and return your custom selector implementation
           return new CustomSelector(config);
       }
   }
   
   public class CustomSelector implements UploaderNodeSelector {
       public CustomSelector(Map<String, String> config) {
           // Initialize your selector with the configuration
       }
       
       @Override
       public boolean isPrimaryUploader() {
           // Implement your custom logic
           return /* your decision logic */;
       }
   }
   ```

2. **Register the Provider**

   Create a file at `META-INF/services/com.automq.opentelemetry.exporter.s3.UploaderNodeSelectorProvider` containing the fully qualified class name of your provider:

   ```
   com.example.CustomSelectorProvider
   ```

3. **Configure the Custom Selector**

   ```properties
   automq.telemetry.s3.selector.type=custom-type
   # Any additional parameters your custom selector needs
   ```

### Example Configurations

#### Single Node Setup

```properties
automq.telemetry.exporter.uri=s3://accessKey:secretKey@metrics-bucket?endpoint=https://s3.amazonaws.com
automq.telemetry.s3.cluster.id=my-cluster
automq.telemetry.s3.node.id=1
automq.telemetry.s3.primary.node=true
automq.telemetry.s3.selector.type=controller
```


```properties
```

### Advanced Configuration

| Configuration | Description | Default Value |
|---------------|-------------|---------------|
| `automq.telemetry.exporter.interval.ms` | Export interval (milliseconds) | `60000` |
| `automq.telemetry.exporter.otlp.protocol` | OTLP protocol | `grpc` |
| `automq.telemetry.exporter.otlp.compression` | OTLP compression method | `none` |
| `automq.telemetry.exporter.otlp.timeout.ms` | OTLP timeout (milliseconds) | `30000` |
| `automq.telemetry.s3.cluster.id` | Cluster ID for S3 metrics | `automq-cluster` |
| `automq.telemetry.s3.node.id` | Node ID for S3 metrics | `0` |
| `automq.telemetry.s3.primary.node` | Whether this node should upload metrics | `false` |
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

# S3 Metrics export (optional)
# automq.telemetry.exporter.uri=s3://access-key:secret-key@my-bucket.s3.amazonaws.com
# automq.telemetry.s3.cluster.id=production-cluster
# automq.telemetry.s3.node.id=${NODE_ID}
# automq.telemetry.s3.primary.node=true (only for one node in the cluster)

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
