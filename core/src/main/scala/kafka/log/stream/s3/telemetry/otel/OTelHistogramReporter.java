/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
