package com.automq.opentelemetry.exporter;

import com.automq.opentelemetry.TelemetryConstants;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.metrics.export.MetricReader;

public class PrometheusMetricsExporter implements MetricsExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusMetricsExporter.class);
    private final String host;
    private final int port;
    private final Set<String> baseLabelKeys;

    public PrometheusMetricsExporter(String host, int port, List<Pair<String, String>> baseLabels) {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Illegal Prometheus host");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("Illegal Prometheus port");
        }
        this.host = host;
        this.port = port;
        this.baseLabelKeys = baseLabels.stream().map(Pair::getKey).collect(Collectors.toSet());
        LOGGER.info("PrometheusMetricsExporter initialized with host: {}, port: {}", host, port);
    }

    @Override
    public MetricReader asMetricReader() {
        return PrometheusHttpServer.builder()
            .setHost(host)
            .setPort(port)
            // This filter is to align with the original behavior, allowing only specific resource attributes
            // to be converted to prometheus labels.
            .setAllowedResourceAttributesFilter(resourceAttributeKey ->
                TelemetryConstants.PROMETHEUS_JOB_KEY.equals(resourceAttributeKey)
                    || TelemetryConstants.PROMETHEUS_INSTANCE_KEY.equals(resourceAttributeKey)
                    || TelemetryConstants.HOST_NAME_KEY.equals(resourceAttributeKey)
                    || baseLabelKeys.contains(resourceAttributeKey))
            .build();
    }
}
