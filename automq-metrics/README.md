# AutoMQ automq-metrics Module

## Module Structure

```
com.automq.opentelemetry/
├── AutoMQTelemetryManager.java    # Main management class for initialization and lifecycle
├── TelemetryConstants.java        # Constants definition
├── common/
│   ├── OTLPCompressionType.java   # OTLP compression types
│   └── OTLPProtocol.java          # OTLP protocol types
├── exporter/
│   ├── MetricsExporter.java       # Exporter interface
│   ├── MetricsExportConfig.java   # Export configuration
│   ├── MetricsExporterProvider.java # Exporter factory provider
│   ├── MetricsExporterType.java   # Exporter type enumeration
│   ├── MetricsExporterURI.java    # URI parser for exporters
│   ├── OTLPMetricsExporter.java   # OTLP exporter implementation
│   ├── PrometheusMetricsExporter.java # Prometheus exporter implementation
│   └── s3/                        # S3 metrics exporter implementation
│       ├── CompressionUtils.java  # Utility for data compression
│       ├── PrometheusUtils.java   # Utilities for Prometheus format
│       ├── S3MetricsExporter.java # S3 metrics exporter implementation
│       └── S3MetricsExporterAdapter.java # Adapter to handle S3 metrics export
└── yammer/
    ├── DeltaHistogram.java        # Delta histogram implementation
    ├── OTelMetricUtils.java       # OpenTelemetry metrics utilities
    ├── YammerMetricsProcessor.java # Yammer metrics processor
    └── YammerMetricsReporter.java  # Yammer metrics reporter
```

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
│      ── OTLPMetricsExporter.java   # OTLP exporter implementation
│   ├── PrometheusMetricsExporter.java # Prometheus exporter implementation
│   └── s3/                        # S3 metrics exporter implementation
│       ├── CompressionUtils.java  # Utility for data compression
│       ├── PrometheusUtils.java   # Utilities for Prometheus format
│       ├── S3MetricsConfig.java   # Configuration interface
│       ├── S3MetricsExporter.java # S3 metrics exporter implementation
│       ├── S3MetricsExporterAdapter.java # Adapter to handle S3 metrics export
│       ├── LeaderNodeSelector.java # Interface for node selection logic
│       └── LeaderNodeSelectors.java # Factory for node selector implementations
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
import com.automq.opentelemetry.exporter.MetricsExportConfig;

// Implement MetricsExportConfig
public class MyMetricsExportConfig implements MetricsExportConfig {
    @Override
    public String clusterId() { return "my-cluster"; }
    
    @Override
    public boolean isLeader() { return true; }
    
    @Override
    public int nodeId() { return 1; }
    
    @Override
    public ObjectStorage objectStorage() { 
        // Return your object storage instance for S3 exports
        return myObjectStorage; 
    }
    
    @Override
    public List<Pair<String, String>> baseLabels() {
        return Arrays.asList(
            Pair.of("environment", "production"),
            Pair.of("region", "us-east-1")
        );
    }
    
    @Override
    public int intervalMs() { return 60000; } // 60 seconds
}

// Create export configuration
MetricsExportConfig config = new MyMetricsExportConfig();

// Initialize telemetry manager singleton
AutoMQTelemetryManager manager = AutoMQTelemetryManager.initializeInstance(
    "prometheus://localhost:9090",  // exporter URI
    "automq-kafka",                 // service name
    "broker-1",                     // instance ID
    config                          // export config
);

// Start Yammer metrics reporting (optional)
MetricsRegistry yammerRegistry = // Get Kafka's Yammer registry
manager.startYammerMetricsReporter(yammerRegistry);

// Application running...

// Shutdown telemetry system
AutoMQTelemetryManager.shutdownInstance();
```

### 2. Get Meter Instance

```java
// Get the singleton instance
AutoMQTelemetryManager manager = AutoMQTelemetryManager.getInstance();

// Get Meter for custom metrics
Meter meter = manager.getMeter();

// Create custom metrics
LongCounter requestCounter = meter
    .counterBuilder("http_requests_total")
    .setDescription("Total number of HTTP requests")
    .build();

requestCounter.add(1, Attributes.of(AttributeKey.stringKey("method"), "GET"));
```

## Configuration

### Basic Configuration

Configuration is provided through the `MetricsExportConfig` interface and constructor parameters:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `exporterUri` | Metrics exporter URI | `prometheus://localhost:9090` |
| `serviceName` | Service name for telemetry | `automq-kafka` |
| `instanceId` | Unique service instance ID | `broker-1` |
| `config` | MetricsExportConfig implementation | See example above |

### Exporter Configuration

For direct telemetry API usage, configuration is provided through MetricsExportConfig and constructor parameters. Kafka brokers additionally support the authentication properties documented below:

1. **Exporter URI**: Determines the export destination and protocol
2. **MetricsExportConfig**: Provides cluster information, intervals, and base labels
3. **Constructor parameters**: Service name and instance ID

#### Prometheus Exporter
```java
// Use prometheus:// URI scheme
AutoMQTelemetryManager manager = AutoMQTelemetryManager.initializeInstance(
    "prometheus://localhost:9090",
    "automq-kafka", 
    "broker-1",
    config
);
```

#### Prometheus Basic Authentication (Kafka Broker)

Kafka brokers support optional HTTP Basic authentication for the built-in Prometheus endpoint. Authentication is **disabled by default** to preserve the behavior of existing deployments.

Configure these properties in the Kafka broker configuration:

```properties
s3.telemetry.metrics.exporter.uri=prometheus://?host=127.0.0.1&port=9090
s3.telemetry.metrics.prometheus.auth.type=basic
s3.telemetry.metrics.prometheus.auth.username=prometheus
s3.telemetry.metrics.prometheus.auth.password=<your-secret-password>
```

Replace `<your-secret-password>` with a securely provisioned secret. The password setting accepts a password **value**, not the name or path of a secret file. Do not embed credentials in `s3.telemetry.metrics.exporter.uri`, since exporter URIs may be logged.

| Property | Behavior |
|----------|----------|
| `s3.telemetry.metrics.prometheus.auth.type` | `none` (default) or `basic` |
| `s3.telemetry.metrics.prometheus.auth.username` | Required nonblank username for `basic`; must not contain `:` |
| `s3.telemetry.metrics.prometheus.auth.password` | Required nonempty password for `basic`; handled as a Kafka `PASSWORD` config |

When Basic authentication is enabled:

- Requests for `/metrics` without credentials or with invalid credentials receive HTTP `401` and a `WWW-Authenticate` challenge.
- Requests with valid credentials can scrape `/metrics`.
- The exact `/` and `/-/healthy` paths remain public for compatibility with existing health checks.
- Other request paths also require authentication; do not assume they are public health endpoints.
- Credentials are fixed at startup. Update the broker configuration and restart the broker to rotate credentials.

Prometheus scraping configuration for a collector in the **same network namespace** as the broker:

```yaml
scrape_configs:
  - job_name: automq
    static_configs:
      - targets: ['127.0.0.1:9090']
    metrics_path: /metrics
    basic_auth:
      username: prometheus
      password_file: /etc/prometheus/secrets/automq-password
```

The `password_file` shown above belongs to **Prometheus**; it is not an AutoMQ broker property. Provision the same credential securely for both components. Ensure the Prometheus process can read the file and that the file is protected from other users.

**Transport security and deployment:**

- Basic authentication does **not** encrypt passwords in transit. For scrapes across an untrusted network, use HTTPS through a reverse proxy, Kubernetes sidecar/ingress, or equivalent TLS termination, and restrict access with network controls.
- **VM/IaaS:** A loopback-bound endpoint works with a local collector or a secure local reverse proxy. Protect the broker configuration and Prometheus password file with restrictive filesystem permissions.
- **Kubernetes:** A collector in a different pod cannot reach the broker's `127.0.0.1` listener. For cross-pod scraping, expose a deliberately chosen interface through an authenticated, TLS-protected endpoint or proxy. Use Kubernetes Secrets to supply credentials and render them into the broker configuration securely.
- Avoid exposing a plain HTTP listener bound to `0.0.0.0` on an untrusted network. If TLS is terminated by a proxy, limit direct access to the broker listener so clients cannot bypass the proxy.

This configuration is wired through the Kafka broker telemetry startup path. Separately initialized telemetry components, including Kafka Connect components using the direct `AutoMQTelemetryManager.initializeInstance(...)` API, are not automatically protected by these broker settings.

#### OTLP Exporter  
```java
// Use otlp:// URI scheme with optional query parameters
AutoMQTelemetryManager manager = AutoMQTelemetryManager.initializeInstance(
    "otlp://localhost:4317?protocol=grpc&compression=gzip&timeout=30000",
    "automq-kafka",
    "broker-1", 
    config
);
```

#### S3 Metrics Exporter
```java
// Use ops:// URI scheme
AutoMQTelemetryManager manager = AutoMQTelemetryManager.initializeInstance(
    "ops://?",
    "automq-kafka",
    "broker-1",
    config // config.clusterId(), nodeId(), isLeader() used for S3 export
);
```

The `ops://?` URI selects the object-storage exporter. The bucket, endpoint, and credentials are provided by `MetricsExportConfig.objectStorage()`.

Example usage with S3 exporter:

```java
// Implementation for S3 export configuration  
public class S3MetricsExportConfig implements MetricsExportConfig {
    private final ObjectStorage objectStorage;
    
    public S3MetricsExportConfig(ObjectStorage objectStorage) {
        this.objectStorage = objectStorage;
    }
    
    @Override
    public String clusterId() { return "my-kafka-cluster"; }
    
    @Override
    public boolean isLeader() { 
        // Only one node in the cluster should return true
        return isCurrentNodeLeader(); 
    }
    
    @Override
    public int nodeId() { return 1; }
    
    @Override
    public ObjectStorage objectStorage() { return objectStorage; }
    
    @Override
    public List<Pair<String, String>> baseLabels() {
        return Arrays.asList(Pair.of("environment", "production"));
    }
    
    @Override
    public int intervalMs() { return 60000; }
}

// Initialize telemetry manager with S3 export
ObjectStorage objectStorage = // Create your object storage instance
MetricsExportConfig config = new S3MetricsExportConfig(objectStorage);

AutoMQTelemetryManager manager = AutoMQTelemetryManager.initializeInstance(
    "ops://?",
    "automq-kafka",
    "broker-1", 
    config
);

// Application running...

// Shutdown telemetry system
AutoMQTelemetryManager.shutdownInstance();
```

### JMX Metrics Configuration

Define JMX metrics collection rules through YAML configuration files:

```java
AutoMQTelemetryManager manager = AutoMQTelemetryManager.initializeInstance(
    exporterUri, serviceName, instanceId, config
);

// Set JMX config paths after initialization
manager.setJmxConfigPaths("/jmx-config.yaml,/kafka-jmx.yaml");
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

```java
public class ProductionMetricsConfig implements MetricsExportConfig {
    @Override
    public String clusterId() { return "production-cluster"; }
    
    @Override
    public boolean isLeader() { 
        // Implement your leader election logic
        return isCurrentNodeController(); 
    }
    
    @Override
    public int nodeId() { return getCurrentNodeId(); }
    
    @Override
    public ObjectStorage objectStorage() { 
        return productionObjectStorage; 
    }
    
    @Override
    public List<Pair<String, String>> baseLabels() {
        return Arrays.asList(
            Pair.of("environment", "production"),
            Pair.of("region", System.getenv("AWS_REGION")),
            Pair.of("version", getApplicationVersion())
        );
    }
    
    @Override
    public int intervalMs() { return 60000; } // 1 minute
}

// Initialize for production
AutoMQTelemetryManager manager = AutoMQTelemetryManager.initializeInstance(
    "prometheus://0.0.0.0:9090",        // Or S3 URI for object storage export
    "automq-kafka",
    System.getenv("HOSTNAME"),
    new ProductionMetricsConfig()
);
```

### 2. Development Environment Configuration

```java
public class DevelopmentMetricsConfig implements MetricsExportConfig {
    @Override
    public String clusterId() { return "dev-cluster"; }
    
    @Override
    public boolean isLeader() { return true; } // Single node in dev
    
    @Override
    public int nodeId() { return 1; }
    
    @Override
    public ObjectStorage objectStorage() { return null; } // Not needed for OTLP
    
    @Override
    public List<Pair<String, String>> baseLabels() {
        return Arrays.asList(Pair.of("environment", "development"));
    }
    
    @Override
    public int intervalMs() { return 10000; } // 10 seconds for faster feedback
}

// Initialize for development  
AutoMQTelemetryManager manager = AutoMQTelemetryManager.initializeInstance(
    "otlp://localhost:4317",
    "automq-kafka-dev",
    "local-dev",
    new DevelopmentMetricsConfig()
);
```

### 3. Resource Management
- Set appropriate metric cardinality limits to avoid memory leaks
- Call `shutdown()` method when application closes to release resources
- Monitor exporter health status

## Troubleshooting

### Common Issues

1. **Metrics not exported**
   - Check if exporter URI passed to `initializeInstance()` is correct
   - Verify target endpoint is reachable
   - Check error messages in logs
   - Ensure `MetricsExportConfig.intervalMs()` returns reasonable value

2. **JMX metrics missing**
   - Confirm JMX configuration file path set via `setJmxConfigPaths()` is correct
   - Check YAML configuration file format
   - Verify JMX Bean exists
   - Ensure files are in classpath

3. **High memory usage**
   - Implement cardinality limits in your `MetricsExportConfig`
   - Check for high cardinality labels in `baseLabels()`
   - Consider increasing export interval via `intervalMs()`

### Logging Configuration

Enable debug logging for more information using your logging framework configuration (e.g., logback.xml, log4j2.xml):

```xml
<!-- For Logback -->
<logger name="com.automq.opentelemetry" level="DEBUG" />
<logger name="io.opentelemetry" level="INFO" />
```

## Dependencies

- Java 8+
- OpenTelemetry SDK 1.30+
- Apache Commons Lang3
- SLF4J logging framework

## License

This module is open source under the Apache License 2.0.
