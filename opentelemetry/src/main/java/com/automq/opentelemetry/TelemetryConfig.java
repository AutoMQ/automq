package com.automq.opentelemetry;

import org.apache.commons.lang3.tuple.Pair;

import java.net.InetAddress;
import java.net.UnknownHostException;
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
        return Collections.emptyList();
    }
}
