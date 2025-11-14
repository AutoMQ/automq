package org.apache.kafka.connect.automq.metrics;

public class MetricsConfigConstants {
    public static final String SERVICE_NAME_KEY = "service.name";
    public static final String SERVICE_INSTANCE_ID_KEY = "service.instance.id";
    public static final String S3_CLIENT_ID_KEY = "automq.telemetry.s3.cluster.id";
    /**
     * The URI for configuring metrics exporters. e.g. prometheus://localhost:9090, otlp://localhost:4317
     */
    public static final String EXPORTER_URI_KEY = "automq.telemetry.exporter.uri";
    /**
     * The export interval in milliseconds.
     */
    public static final String EXPORTER_INTERVAL_MS_KEY = "automq.telemetry.exporter.interval.ms";
    /**
     * The cardinality limit for any single metric.
     */
    public static final String METRIC_CARDINALITY_LIMIT_KEY = "automq.telemetry.metric.cardinality.limit";
    public static final int DEFAULT_METRIC_CARDINALITY_LIMIT = 20000;

    public static final String TELEMETRY_METRICS_BASE_LABELS_CONFIG = "automq.telemetry.metrics.base.labels";
    public static final String TELEMETRY_METRICS_BASE_LABELS_DOC = "The base labels that will be added to all metrics. The format is key1=value1,key2=value2.";
    
    public static final String S3_BUCKET = "automq.telemetry.s3.bucket";
    public static final String S3_BUCKETS_DOC = "The buckets url with format 0@s3://$bucket?region=$region. \n" +
        "the full url format for s3 is 0@s3://$bucket?region=$region[&endpoint=$endpoint][&pathStyle=$enablePathStyle][&authType=$authType][&accessKey=$accessKey][&secretKey=$secretKey][&checksumAlgorithm=$checksumAlgorithm]" +
        "- pathStyle: true|false. The object storage access path style. When using MinIO, it should be set to true.\n" +
        "- authType: instance|static. When set to instance, it will use instance profile to auth. When set to static, it will get accessKey and secretKey from the url or from system environment KAFKA_S3_ACCESS_KEY/KAFKA_S3_SECRET_KEY.";

}
