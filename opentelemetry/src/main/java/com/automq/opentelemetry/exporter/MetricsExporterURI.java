package com.automq.opentelemetry.exporter;

import com.automq.opentelemetry.TelemetryConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses the exporter URI and creates the corresponding MetricsExporter instances.
 */
public class MetricsExporterURI {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsExporterURI.class);

    private final List<MetricsExporter> metricsExporters;

    private MetricsExporterURI(List<MetricsExporter> metricsExporters) {
        this.metricsExporters = metricsExporters != null ? metricsExporters : new ArrayList<>();
    }

    public List<MetricsExporter> getMetricsExporters() {
        return metricsExporters;
    }

    public static MetricsExporterURI parse(TelemetryConfig config) {
        String uriStr = config.getExporterUri();
        if (StringUtils.isBlank(uriStr)) {
            LOGGER.info("Metrics exporter URI is not configured, no metrics will be exported.");
            return new MetricsExporterURI(Collections.emptyList());
        }

        // Support multiple exporters separated by comma
        String[] exporterUris = uriStr.split(",");
        if (exporterUris.length == 0) {
            return new MetricsExporterURI(Collections.emptyList());
        }

        List<MetricsExporter> exporters = new ArrayList<>();
        for (String uri : exporterUris) {
            if (StringUtils.isBlank(uri)) {
                continue;
            }
            MetricsExporter exporter = parseExporter(config, uri.trim());
            if (exporter != null) {
                exporters.add(exporter);
            }
        }
        return new MetricsExporterURI(exporters);
    }

    public static MetricsExporter parseExporter(TelemetryConfig config, String uriStr) {
        try {
            URI uri = new URI(uriStr);
            String type = uri.getScheme();
            if (StringUtils.isBlank(type)) {
                LOGGER.error("Invalid metrics exporter URI: {}, exporter scheme is missing", uriStr);
                return null;
            }

            Map<String, List<String>> queries = parseQueryParameters(uri);
            return parseExporter(config, type, queries, uri);
        } catch (Exception e) {
            LOGGER.warn("Parse metrics exporter URI {} failed", uriStr, e);
            return null;
        }
    }

    public static MetricsExporter parseExporter(TelemetryConfig config, String type,
                                              Map<String, List<String>> queries, URI uri) {
        MetricsExporterType exporterType = MetricsExporterType.fromString(type);
        switch (exporterType) {
            case PROMETHEUS:
                return buildPrometheusExporter(config, queries, uri);
            case OTLP:
                return buildOtlpExporter(config, queries, uri);
            default:
                LOGGER.warn("Unsupported metrics exporter type: {}", type);
                return null;
        }
    }

    private static MetricsExporter buildPrometheusExporter(TelemetryConfig config,
                                                         Map<String, List<String>> queries, URI uri) {
        // Use query parameters if available, otherwise fall back to URI authority or config defaults
        String host = getStringFromQuery(queries, "host", uri.getHost());
        if (StringUtils.isBlank(host)) {
            host = "localhost";
        }

        int port = uri.getPort();
        if (port <= 0) {
            String portStr = getStringFromQuery(queries, "port", null);
            if (StringUtils.isNotBlank(portStr)) {
                try {
                    port = Integer.parseInt(portStr);
                } catch (NumberFormatException e) {
                    LOGGER.warn("Invalid port in query parameters: {}, using default", portStr);
                    port = 9090;
                }
            } else {
                port = 9090;
            }
        }

        return new PrometheusMetricsExporter(host, port, config.getBaseLabels());
    }

    private static MetricsExporter buildOtlpExporter(TelemetryConfig config,
                                                     Map<String, List<String>> queries, URI uri) {
        // Get endpoint from query parameters or construct from URI
        String endpoint = getStringFromQuery(queries, "endpoint", null);
        if (StringUtils.isBlank(endpoint)) {
            endpoint = uri.getScheme() + "://" + uri.getAuthority();
        }

        // Get protocol from query parameters or config
        String protocol = getStringFromQuery(queries, "protocol", config.getOtlpProtocol());

        // Get compression from query parameters or config
        String compression = getStringFromQuery(queries, "compression", config.getOtlpCompression());

        // Get timeout from query parameters or config
        long timeoutMs = config.getOtlpTimeoutMs();
        String timeoutStr = getStringFromQuery(queries, "timeout", null);
        if (StringUtils.isNotBlank(timeoutStr)) {
            try {
                timeoutMs = Long.parseLong(timeoutStr);
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid timeout in query parameters: {}, using config default", timeoutStr);
            }
        }

        return new OTLPMetricsExporter(config.getExporterIntervalMs(), endpoint, protocol, compression, timeoutMs);
    }

    private static Map<String, List<String>> parseQueryParameters(URI uri) {
        Map<String, List<String>> queries = new HashMap<>();
        String query = uri.getQuery();
        if (StringUtils.isNotBlank(query)) {
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                String[] keyValue = pair.split("=", 2);
                if (keyValue.length == 2) {
                    String key = keyValue[0];
                    String value = keyValue[1];
                    queries.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
                }
            }
        }
        return queries;
    }

    private static String getStringFromQuery(Map<String, List<String>> queries, String key, String defaultValue) {
        List<String> values = queries.get(key);
        if (values != null && !values.isEmpty()) {
            return values.get(0);
        }
        return defaultValue;
    }
}
