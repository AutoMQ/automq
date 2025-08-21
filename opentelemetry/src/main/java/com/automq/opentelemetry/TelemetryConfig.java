package com.automq.opentelemetry;

import com.automq.stream.s3.operator.BucketURI;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Provides strongly-typed access to telemetry configuration properties.
 * This class centralizes configuration handling for the telemetry module.
 */
public class TelemetryConfig {

    private final Properties props;

    public TelemetryConfig(Properties props) {
        this.props = props != null ? props : new Properties();
    }

    public String getExporterUri() {
        return props.getProperty(TelemetryConstants.EXPORTER_URI_KEY, "");
    }

    public long getExporterIntervalMs() {
        return Long.parseLong(props.getProperty(TelemetryConstants.EXPORTER_INTERVAL_MS_KEY, "60000"));
    }

    public String getOtlpProtocol() {
        return props.getProperty(TelemetryConstants.EXPORTER_OTLP_PROTOCOL_KEY, "grpc");
    }

    public String getOtlpCompression() {
        return props.getProperty(TelemetryConstants.EXPORTER_OTLP_COMPRESSION_KEY, "none");
    }

    public long getOtlpTimeoutMs() {
        return Long.parseLong(props.getProperty(TelemetryConstants.EXPORTER_OTLP_TIMEOUT_MS_KEY, "30000"));
    }

    public String getServiceName() {
        return props.getProperty(TelemetryConstants.SERVICE_NAME_KEY, "unknown-service");
    }

    public String getInstanceId() {
        return props.getProperty(TelemetryConstants.SERVICE_INSTANCE_ID_KEY, "unknown-instance");
    }

    public List<String> getJmxConfigPaths() {
        String paths = props.getProperty(TelemetryConstants.JMX_CONFIG_PATH_KEY, "");
        if (paths.isEmpty()) {
            return Collections.emptyList();
        }
        return Stream.of(paths.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    public int getMetricCardinalityLimit() {
        return Integer.parseInt(props.getProperty(TelemetryConstants.METRIC_CARDINALITY_LIMIT_KEY,
                String.valueOf(TelemetryConstants.DEFAULT_METRIC_CARDINALITY_LIMIT)));
    }

    public String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown-host";
        }
    }

    /**
     * A placeholder for custom labels which might be passed in a different way.
     * In a real scenario, this might come from a properties prefix.
     */
    public List<Pair<String, String>> getBaseLabels() {
        // This part is hard to abstract without a clear config pattern.
        // Assuming for now it's empty. The caller can extend this class
        // or the manager can have a method to add more labels.
        String baseLabels = props.getProperty(TelemetryConstants.TELEMETRY_METRICS_BASE_LABELS_CONFIG);
        if (StringUtils.isBlank(baseLabels)) {
            return Collections.emptyList();
        }
        List<Pair<String, String>> labels = new ArrayList<>();
        for (String label : baseLabels.split(",")) {
            String[] kv = label.split("=");
            if (kv.length != 2) {
                continue;
            }
            labels.add(Pair.of(kv[0], kv[1]));
        }
        return labels;
    }

    public BucketURI getMetricsBucket() {
        String metricsBucket = props.getProperty(TelemetryConstants.S3_BUCKET, "");
        if (StringUtils.isNotBlank(metricsBucket)) {
            List<BucketURI> bucketList = BucketURI.parseBuckets(metricsBucket);
            if (!bucketList.isEmpty()) {
                return bucketList.get(0);
            }
        }
        return null;
    }
    
    /**
     * Get a property value with a default.
     * 
     * @param key The property key.
     * @param defaultValue The default value if the property is not set.
     * @return The property value or default value.
     */
    public String getProperty(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }
    
    /**
     * Get the S3 cluster ID.
     * 
     * @return The S3 cluster ID.
     */
    public String getS3ClusterId() {
        return props.getProperty(TelemetryConstants.S3_CLUSTER_ID_KEY, "automq-cluster");
    }
    
    /**
     * Get the S3 node ID.
     * 
     * @return The S3 node ID.
     */
    public int getS3NodeId() {
        return Integer.parseInt(props.getProperty(TelemetryConstants.S3_NODE_ID_KEY, "0"));
    }
    
    /**
     * Check if this node is a primary S3 metrics uploader.
     * 
     * @return True if this node is a primary uploader, false otherwise.
     */
    public boolean isS3PrimaryNode() {
        return Boolean.parseBoolean(props.getProperty(TelemetryConstants.S3_PRIMARY_NODE_KEY, "false"));
    }
}
