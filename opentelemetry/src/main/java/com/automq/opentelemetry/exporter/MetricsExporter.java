package com.automq.opentelemetry.exporter;

import io.opentelemetry.sdk.metrics.export.MetricReader;

/**
 * An interface for metrics exporters, which can be converted to an OpenTelemetry MetricReader.
 */
public interface MetricsExporter {
    MetricReader asMetricReader();
}
