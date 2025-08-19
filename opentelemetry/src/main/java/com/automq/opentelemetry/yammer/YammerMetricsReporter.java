package com.automq.opentelemetry.yammer;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;
import io.opentelemetry.api.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * A listener that bridges Yammer Histogram metrics to OpenTelemetry.
 * It listens for new metrics added to a MetricsRegistry and creates corresponding
 * OTel gauge metrics for mean and max values of histograms.
 */
public class YammerMetricsReporter implements MetricsRegistryListener, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(YammerMetricsReporter.class);
    private final MetricsRegistry metricsRegistry;
    private final YammerMetricsProcessor metricsProcessor;
    private volatile Meter meter;

    public YammerMetricsReporter(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        this.metricsProcessor = new YammerMetricsProcessor();
    }

    public void start(Meter meter) {
        this.meter = meter;
        this.metricsProcessor.init(meter);
        metricsRegistry.addListener(this);
        LOGGER.info("OTelHistogramReporter started");
    }

    @Override
    public void onMetricAdded(MetricName name, Metric metric) {
        if (OTelMetricUtils.isInterestedMetric(name)) {
            if (this.meter == null) {
                LOGGER.info("Not initialized yet, skipping metric: {}", name);
                return;
            }
            try {
                metric.processWith(this.metricsProcessor, name, null);
            } catch (Throwable t) {
                LOGGER.error("Failed to process metric: {}", name, t);
            }
        }
    }

    @Override
    public void onMetricRemoved(MetricName name) {
        try {
            this.metricsProcessor.remove(name);
        } catch (Throwable ignored) {

        }
    }

    @Override
    public void close() throws IOException {
        try {
            // Remove this reporter as a listener from the metrics registry
            metricsRegistry.removeListener(this);
            LOGGER.info("YammerMetricsReporter stopped and removed from metrics registry");
        } catch (Exception e) {
            LOGGER.error("Error while closing YammerMetricsReporter", e);
            throw new IOException("Failed to close YammerMetricsReporter", e);
        }
    }
}