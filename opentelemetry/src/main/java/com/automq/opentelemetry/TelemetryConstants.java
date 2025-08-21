package com.automq.opentelemetry;

import io.opentelemetry.api.common.AttributeKey;

/**
 * Constants for telemetry, including configuration keys, attribute keys, and default values.
 */
public class TelemetryConstants {

    //################################################################
    // Service and Resource Attributes
    //################################################################
    public static final String SERVICE_NAME_KEY = "service.name";
    public static final String SERVICE_INSTANCE_ID_KEY = "service.instance.id";
    public static final String HOST_NAME_KEY = "host.name";
    public static final String TELEMETRY_SCOPE_NAME = "automq_for_kafka";

    //################################################################
    // Exporter Configuration Keys
    //################################################################
    /**
     * The URI for configuring metrics exporters. e.g. prometheus://localhost:9090, otlp://localhost:4317
     */
    public static final String EXPORTER_URI_KEY = "automq.telemetry.exporter.uri";
    /**
     * The export interval in milliseconds.
     */
    public static final String EXPORTER_INTERVAL_MS_KEY = "automq.telemetry.exporter.interval.ms";
    /**
     * The OTLP protocol, can be "grpc" or "http/protobuf".
     */
    public static final String EXPORTER_OTLP_PROTOCOL_KEY = "automq.telemetry.exporter.otlp.protocol";
    /**
     * The OTLP compression method, can be "gzip" or "none".
     */
    public static final String EXPORTER_OTLP_COMPRESSION_KEY = "automq.telemetry.exporter.otlp.compression";
    /**
     * The timeout for OTLP exporter in milliseconds.
     */
    public static final String EXPORTER_OTLP_TIMEOUT_MS_KEY = "automq.telemetry.exporter.otlp.timeout.ms";
    /**
     * A comma-separated list of JMX configuration file paths (classpath resources).
     */
    public static final String JMX_CONFIG_PATH_KEY = "automq.telemetry.jmx.config.paths";

    //################################################################
    // Metric Configuration
    //################################################################
    /**
     * The cardinality limit for any single metric.
     */
    public static final String METRIC_CARDINALITY_LIMIT_KEY = "automq.telemetry.metric.cardinality.limit";
    public static final int DEFAULT_METRIC_CARDINALITY_LIMIT = 20000;

    public static final String TELEMETRY_METRICS_BASE_LABELS_CONFIG = "automq.telemetry.metrics.base.labels";
    public static final String TELEMETRY_METRICS_BASE_LABELS_DOC = "The base labels that will be added to all metrics. The format is key1=value1,key2=value2.";


    //################################################################
    // Prometheus specific Attributes, for compatibility
    //################################################################
    public static final String PROMETHEUS_JOB_KEY = "job";
    public static final String PROMETHEUS_INSTANCE_KEY = "instance";

    //################################################################
    // Custom Kafka-related Attribute Keys
    //################################################################
    public static final AttributeKey<Long> STREAM_ID_KEY = AttributeKey.longKey("streamId");
    public static final AttributeKey<Long> START_OFFSET_KEY = AttributeKey.longKey("startOffset");
    public static final AttributeKey<Long> END_OFFSET_KEY = AttributeKey.longKey("endOffset");
    
    //################################################################
    // S3 Metrics Exporter Configuration
    //################################################################
    
    public static final String S3_BUCKET = "automq.telemetry.s3.bucket";
    
    /**
     * The cluster ID for S3 metrics.
     */
    public static final String S3_CLUSTER_ID_KEY = "automq.telemetry.s3.cluster.id";
    /**
     * The node ID for S3 metrics.
     */
    public static final String S3_NODE_ID_KEY = "automq.telemetry.s3.node.id";
    /**
     * Whether this node is the primary uploader for S3 metrics.
     */
    public static final String S3_PRIMARY_NODE_KEY = "automq.telemetry.s3.primary.node";
}
