package com.automq.opentelemetry.exporter;

import com.automq.opentelemetry.TelemetryConfig;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Service Provider Interface that allows extending the available metrics exporters
 * without modifying the core AutoMQ OpenTelemetry module.
 */
public interface MetricsExporterProvider {

    /**
     * @param scheme exporter scheme (e.g. "rw")
     * @return true if this provider can create an exporter for the supplied scheme
     */
    boolean supports(String scheme);

    /**
     * Creates a metrics exporter for the provided URI.
     *
     * @param config          telemetry configuration
     * @param uri             original exporter URI
     * @param queryParameters parsed query parameters from the URI
     * @return a MetricsExporter instance, or {@code null} if unable to create one
     */
    MetricsExporter create(TelemetryConfig config, URI uri, Map<String, List<String>> queryParameters);
}
