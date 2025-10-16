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

package com.automq.stream.s3.metrics;

import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.context.Context;

public class Metrics {
    private static final Metrics INSTANCE = new Metrics();
    private Meter meter;
    private MetricsConfig globalConfig;
    private final Queue<Setup> waitingSetups = new ConcurrentLinkedQueue<>();

    public static Metrics instance() {
        return INSTANCE;
    }

    public synchronized void setup(Meter meter, MetricsConfig metricsConfig) {
        this.meter = meter;
        this.globalConfig = metricsConfig;
        setup0();
    }

    public HistogramBundle histogram(String name, String desc, String unit) {
        return new HistogramBundle(name, desc, unit);
    }

    public LongCounter counter(Function<Meter, LongCounter> newFunc) {
        return new LazyLongCounter(newFunc);
    }

    public LongGaugeBundle longGauge(String name, String desc, String unit) {
        return new LongGaugeBundle(name, desc, unit);
    }

    public DoubleGaugeBundle doubleGauge(String name, String desc, String unit) {
        return new DoubleGaugeBundle(name, desc, unit);
    }

    private synchronized void setup0() {
        if (meter == null) {
            return;
        }
        for (; ; ) {
            Setup setup = waitingSetups.poll();
            if (setup == null) {
                break;
            }
            setup.setup();
        }
    }

    interface Setup {
        void setup();
    }

    public class HistogramBundle implements Setup {
        private final List<Histogram> histograms = new CopyOnWriteArrayList<>();
        private final String name;
        private final String desc;
        private final String unit;

        private ObservableLongGauge count;
        private ObservableLongGauge sum;
        private ObservableDoubleGauge histP50Value;
        private ObservableDoubleGauge histP99Value;
        private ObservableDoubleGauge histMaxValue;

        @SuppressWarnings("this-escape")
        public HistogramBundle(String name, String desc, String unit) {
            this.name = name;
            this.desc = desc;
            this.unit = unit;
            waitingSetups.add(this);
            setup0();
        }

        public synchronized DeltaHistogram histogram(MetricsLevel level, Attributes attributes) {
            Histogram histogram = new Histogram(level, attributes);
            histograms.add(histogram);
            histogram.setup();
            return histogram.histogram;
        }

        public synchronized void setup() {
            histograms.forEach(Histogram::setup);
            this.count = meter.gaugeBuilder(name + S3StreamMetricsConstant.COUNT_METRIC_NAME_SUFFIX)
                .setDescription(desc + " (count)")
                .ofLongs()
                .buildWithCallback(result -> {
                    histograms.forEach(histogram -> {
                        if (histogram.shouldRecord()) {
                            result.record(histogram.histogram.count(), histogram.attributes());
                        }
                    });
                });
            this.sum = meter.gaugeBuilder(name + S3StreamMetricsConstant.SUM_METRIC_NAME_SUFFIX)
                .setDescription(desc + " (sum)")
                .ofLongs()
                .setUnit(unit)
                .buildWithCallback(result -> {
                    histograms.forEach(histogram -> {
                        if (histogram.shouldRecord()) {
                            result.record(histogram.histogram.sum(), histogram.attributes());
                        }
                    });
                });
            this.histP50Value = meter.gaugeBuilder(name + S3StreamMetricsConstant.P50_METRIC_NAME_SUFFIX)
                .setDescription(desc + " (50th percentile)")
                .setUnit(unit)
                .buildWithCallback(result -> {
                    histograms.forEach(histogram -> {
                        if (histogram.shouldRecord()) {
                            result.record(histogram.histogram.p50(), histogram.attributes());
                        }
                    });
                });
            this.histP99Value = meter.gaugeBuilder(name + S3StreamMetricsConstant.P99_METRIC_NAME_SUFFIX)
                .setDescription(desc + " (99th percentile)")
                .setUnit(unit)
                .buildWithCallback(result -> {
                    histograms.forEach(histogram -> {
                        if (histogram.shouldRecord()) {
                            result.record(histogram.histogram.p99(), histogram.attributes());
                        }
                    });
                });
            this.histMaxValue = meter.gaugeBuilder(name + S3StreamMetricsConstant.MAX_METRIC_NAME_SUFFIX)
                .setDescription(desc + " (max)")
                .setUnit(unit)
                .buildWithCallback(result -> {
                    histograms.forEach(histogram -> {
                        if (histogram.shouldRecord()) {
                            result.record(histogram.histogram.max(), histogram.attributes());
                        }
                    });
                });
        }

        class Histogram {
            final DeltaHistogram histogram;
            final MetricsLevel level;
            final Attributes histogramAttrs;
            Attributes finalAttributes;
            boolean shouldRecord = true;

            public Histogram(MetricsLevel level, Attributes attributes) {
                this.histogram = new DeltaHistogram();
                this.level = level;
                this.histogramAttrs = attributes;
                this.finalAttributes = attributes;
            }

            public Attributes attributes() {
                return finalAttributes;
            }

            public boolean shouldRecord() {
                return shouldRecord;
            }

            public void setup() {
                if (meter == null) {
                    return;
                }
                this.finalAttributes = Attributes.builder()
                    .putAll(globalConfig.getBaseAttributes())
                    .putAll(histogramAttrs)
                    .build();
                this.shouldRecord = level.isWithin(globalConfig.getMetricsLevel());
                histogram.setSnapshotInterval(globalConfig.getMetricsReportIntervalMs());
            }
        }
    }

    public class LongGaugeBundle implements Setup {
        private final List<LongGauge> gauges = new CopyOnWriteArrayList<>();
        private final String name;
        private final String desc;
        private final String unit;

        private ObservableLongGauge instrument;

        @SuppressWarnings("this-escape")
        public LongGaugeBundle(String name, String desc, String unit) {
            this.name = name;
            this.desc = desc;
            this.unit = unit;
            waitingSetups.add(this);
            setup0();
        }

        public LongGauge register(MetricsLevel level, Attributes attributes) {
            LongGauge gauge = new LongGauge(level, attributes);
            gauges.add(gauge);
            gauge.setup();
            return gauge;
        }

        public synchronized void setup() {
            gauges.forEach(LongGauge::setup);
            this.instrument = meter.gaugeBuilder(name)
                .setDescription(desc)
                .setUnit(unit)
                .ofLongs()
                .buildWithCallback(measurement -> gauges.forEach(gauge -> gauge.record(measurement)));
        }

        public final class LongGauge implements AutoCloseable {
            private final MetricsLevel level;
            private final Attributes gaugeAttributes;
            private final AtomicLong value = new AtomicLong();
            private final AtomicBoolean hasValue = new AtomicBoolean(false);
            private Attributes finalAttributes = Attributes.empty();
            private volatile boolean shouldRecord = true;

            private LongGauge(MetricsLevel level, Attributes attributes) {
                this.level = level;
                this.gaugeAttributes = attributes;
                this.finalAttributes = attributes;
            }

            private void setup() {
                if (meter != null && globalConfig != null) {
                    this.finalAttributes = Attributes.builder()
                        .putAll(globalConfig.getBaseAttributes())
                        .putAll(gaugeAttributes)
                        .build();
                    this.shouldRecord = level.isWithin(globalConfig.getMetricsLevel());
                } else {
                    this.finalAttributes = gaugeAttributes;
                    this.shouldRecord = true;
                }
            }

            public void record(long newValue) {
                value.set(newValue);
                hasValue.set(true);
            }

            public void clear() {
                hasValue.set(false);
            }

            private void record(ObservableLongMeasurement measurement) {
                if (shouldRecord && hasValue.get()) {
                    measurement.record(value.get(), finalAttributes);
                }
            }

            @Override
            public void close() {
                gauges.remove(this);
                hasValue.set(false);
            }
        }
    }

    public class DoubleGaugeBundle implements Setup {
        private final List<DoubleGauge> gauges = new CopyOnWriteArrayList<>();
        private final String name;
        private final String desc;
        private final String unit;

        private ObservableDoubleGauge instrument;

        @SuppressWarnings("this-escape")
        public DoubleGaugeBundle(String name, String desc, String unit) {
            this.name = name;
            this.desc = desc;
            this.unit = unit;
            waitingSetups.add(this);
            setup0();
        }

        public DoubleGauge register(MetricsLevel level, Attributes attributes) {
            DoubleGauge gauge = new DoubleGauge(level, attributes);
            gauges.add(gauge);
            gauge.setup();
            return gauge;
        }

        public synchronized void setup() {
            gauges.forEach(DoubleGauge::setup);
            this.instrument = meter.gaugeBuilder(name)
                .setDescription(desc)
                .setUnit(unit)
                .buildWithCallback(measurement -> gauges.forEach(gauge -> gauge.record(measurement)));
        }

        public final class DoubleGauge implements AutoCloseable {
            private final MetricsLevel level;
            private final Attributes gaugeAttributes;
            private final AtomicReference<Double> value = new AtomicReference<>(0.0);
            private final AtomicBoolean hasValue = new AtomicBoolean(false);
            private Attributes finalAttributes = Attributes.empty();
            private volatile boolean shouldRecord = true;

            private DoubleGauge(MetricsLevel level, Attributes attributes) {
                this.level = level;
                this.gaugeAttributes = attributes;
                this.finalAttributes = attributes;
            }

            private void setup() {
                if (meter != null && globalConfig != null) {
                    this.finalAttributes = Attributes.builder()
                        .putAll(globalConfig.getBaseAttributes())
                        .putAll(gaugeAttributes)
                        .build();
                    this.shouldRecord = level.isWithin(globalConfig.getMetricsLevel());
                } else {
                    this.finalAttributes = gaugeAttributes;
                    this.shouldRecord = true;
                }
            }

            public void record(double newValue) {
                value.set(newValue);
                hasValue.set(true);
            }

            public void clear() {
                hasValue.set(false);
            }

            private void record(ObservableDoubleMeasurement measurement) {
                if (shouldRecord && hasValue.get()) {
                    measurement.record(value.get(), finalAttributes);
                }
            }

            @Override
            public void close() {
                gauges.remove(this);
                hasValue.set(false);
            }
        }
    }

    public class LazyLongCounter implements Setup, LongCounter {
        private final Function<Meter, LongCounter> newFunc;
        private LongCounter counter = new NoopLongCounter();

        @SuppressWarnings("this-escape")
        public LazyLongCounter(Function<Meter, LongCounter> newFunc) {
            this.newFunc = newFunc;
            waitingSetups.add(this);
            setup0();
        }

        @Override
        public void setup() {
            this.counter = newFunc.apply(meter);
        }

        @Override
        public void add(long value) {
            counter.add(value);
        }

        @Override
        public void add(long value, Attributes attributes) {
            counter.add(value, attributes);
        }

        @Override
        public void add(long value, Attributes attributes, Context context) {
            counter.add(value, attributes, context);
        }
    }
}
