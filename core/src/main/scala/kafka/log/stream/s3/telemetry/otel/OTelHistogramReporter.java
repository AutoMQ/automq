/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.telemetry.otel;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.metrics.Meter;

// This class is responsible for transforming yammer histogram metrics (mean, max) into OTel metrics
public class OTelHistogramReporter implements MetricsRegistryListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(OTelHistogramReporter.class);
    private final MetricsRegistry metricsRegistry;
    private final OTelMetricsProcessor metricsProcessor;
    private volatile Meter meter;

    public OTelHistogramReporter(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        this.metricsProcessor = new OTelMetricsProcessor();
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
}
