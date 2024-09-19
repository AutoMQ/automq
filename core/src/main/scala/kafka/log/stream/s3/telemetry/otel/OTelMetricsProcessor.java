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

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Timer;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import kafka.autobalancer.metricsreporter.metric.MetricsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.UninitializedFieldError;

public class OTelMetricsProcessor implements MetricProcessor<Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OTelMetricsProcessor.class);
    private final Map<String, Map<MetricName, MetricWrapper>> metrics = new ConcurrentHashMap<>();
    private Meter meter = null;

    public void init(Meter meter) {
        this.meter = meter;
    }

    @Override
    public void processMeter(MetricName name, Metered metered, Void unused) {
        throw new UnsupportedOperationException("Meter type is not supported");
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Void unused) {
        throw new UnsupportedOperationException("Counter type is not supported");
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Void unused) {
        processDeltaHistogramMetric(name, new DeltaHistogram(histogram));
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Void unused) {
        processDeltaHistogramMetric(name, new DeltaHistogram(timer));
    }

    private void processDeltaHistogramMetric(MetricName name, DeltaHistogram deltaHistogram) {
        if (meter == null) {
            throw new UninitializedFieldError("Meter is not initialized");
        }
        Map<String, String> tags = MetricsUtils.yammerMetricScopeToTags(name.getScope());
        AttributesBuilder attrBuilder = Attributes.builder();
        if (tags != null) {
            String value = tags.remove(OTelMetricUtils.REQUEST_TAG_KEY);
            if (value != null) {
                tags.put(OTelMetricUtils.TYPE_TAG_KEY, value);
            }
            tags.forEach(attrBuilder::put);
        }
        Attributes attr = attrBuilder.build();
        String otelMetricName = OTelMetricUtils.toMeanMetricName(name);
        metrics.compute(otelMetricName, (k, v) -> {
            if (v == null) {
                v = new ConcurrentHashMap<>();
                final Map<MetricName, MetricWrapper> finalV = v;
                OTelMetricUtils.toMeanGaugeBuilder(meter, name).buildWithCallback(measurement ->
                    finalV.forEach((metricname, metricWrapper) ->
                        measurement.record(metricWrapper.mean(), metricWrapper.getAttr())));
                LOGGER.info("Created delta gauge for metric: {}", otelMetricName);
            }
            v.put(name, new MetricWrapper(attr, deltaHistogram));
            return v;
        });
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Void unused) {
        throw new UnsupportedOperationException("Gauge type is not supported");
    }

    public void remove(MetricName metricName) {
        String otelMetricName = OTelMetricUtils.toMeanMetricName(metricName);
        metrics.compute(otelMetricName, (k, v) -> {
            if (v != null) {
                v.remove(metricName);
                if (v.isEmpty()) {
                    return null;
                }
            }
            return v;
        });
    }

    static class MetricWrapper {
        private final Attributes attr;
        private final DeltaHistogram deltaHistogram;

        public MetricWrapper(Attributes attr, DeltaHistogram deltaHistogram) {
            this.attr = attr;
            this.deltaHistogram = deltaHistogram;
        }

        public Attributes getAttr() {
            return attr;
        }

        public double mean() {
            return this.deltaHistogram.getDeltaMean();
        }
    }
}
