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
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.context.Context;

public class Metrics {
    private static final Metrics INSTANCE = new Metrics();
    private static final String SUM_METRIC_NAME_SUFFIX = "_sum";
    private static final String COUNT_METRIC_NAME_SUFFIX = "_count";
    private static final String P50_METRIC_NAME_SUFFIX = "_50p";
    private static final String P99_METRIC_NAME_SUFFIX = "_99p";
    private static final String MAX_METRIC_NAME_SUFFIX = "_max";
    private Meter meter;
    private MetricsConfig globalConfig;
    private final Queue<Setup> waitingSetups = new ConcurrentLinkedQueue<>();

    private Metrics() {}

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

    public io.opentelemetry.api.metrics.LongCounter counter(Function<Meter, io.opentelemetry.api.metrics.LongCounter> newFunc) {
        return new LazyLongCounter(newFunc);
    }

    public LongCounterBundle longCounter(String name, String desc, String unit) {
        return new LongCounterBundle(name, desc, unit);
    }

    public ObservableLongCounterBundle observableLongCounter(String name, String desc, String unit) {
        return new ObservableLongCounterBundle(name, desc, unit);
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
            this.count = meter.gaugeBuilder(name + COUNT_METRIC_NAME_SUFFIX)
                .setDescription(desc + " (count)")
                .ofLongs()
                .buildWithCallback(result -> {
                    histograms.forEach(histogram -> {
                        if (histogram.shouldRecord()) {
                            result.record(histogram.histogram.cumulativeCount(), histogram.attributes());
                        }
                    });
                });
            this.sum = meter.gaugeBuilder(name + SUM_METRIC_NAME_SUFFIX)
                .setDescription(desc + " (sum)")
                .ofLongs()
                .setUnit(unit)
                .buildWithCallback(result -> {
                    histograms.forEach(histogram -> {
                        if (histogram.shouldRecord()) {
                            result.record(histogram.histogram.cumulativeSum(), histogram.attributes());
                        }
                    });
                });
            this.histP50Value = meter.gaugeBuilder(name + P50_METRIC_NAME_SUFFIX)
                .setDescription(desc + " (50th percentile)")
                .setUnit(unit)
                .buildWithCallback(result -> {
                    histograms.forEach(histogram -> {
                        if (histogram.shouldRecord()) {
                            result.record(histogram.histogram.p50(), histogram.attributes());
                        }
                    });
                });
            this.histP99Value = meter.gaugeBuilder(name + P99_METRIC_NAME_SUFFIX)
                .setDescription(desc + " (99th percentile)")
                .setUnit(unit)
                .buildWithCallback(result -> {
                    histograms.forEach(histogram -> {
                        if (histogram.shouldRecord()) {
                            result.record(histogram.histogram.p99(), histogram.attributes());
                        }
                    });
                });
            this.histMaxValue = meter.gaugeBuilder(name + MAX_METRIC_NAME_SUFFIX)
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

    public class LongCounterBundle implements Setup {
        private final List<LongCounter> counters = new CopyOnWriteArrayList<>();
        private final String name;
        private final String desc;
        private final String unit;

        private volatile io.opentelemetry.api.metrics.LongCounter instrument = new NoopLongCounter();

        @SuppressWarnings("this-escape")
        public LongCounterBundle(String name, String desc, String unit) {
            this.name = name;
            this.desc = desc;
            this.unit = unit;
            waitingSetups.add(this);
            setup0();
        }

        public LongCounter register(MetricsLevel level, Attributes attributes) {
            LongCounter counter = new LongCounter(level, attributes);
            counters.add(counter);
            counter.setup();
            return counter;
        }

        public synchronized void setup() {
            counters.forEach(LongCounter::setup);
            this.instrument = meter.counterBuilder(name)
                .setDescription(desc)
                .setUnit(unit)
                .build();
        }

        public final class LongCounter implements AutoCloseable {
            private final MetricsLevel level;
            private final Attributes counterAttributes;
            private Attributes finalAttributes = Attributes.empty();
            private volatile boolean shouldRecord = true;

            private LongCounter(MetricsLevel level, Attributes attributes) {
                this.level = level;
                this.counterAttributes = attributes;
                this.finalAttributes = attributes;
            }

            private void setup() {
                if (meter != null && globalConfig != null) {
                    this.finalAttributes = Attributes.builder()
                        .putAll(globalConfig.getBaseAttributes())
                        .putAll(counterAttributes)
                        .build();
                    this.shouldRecord = level.isWithin(globalConfig.getMetricsLevel());
                } else {
                    this.finalAttributes = counterAttributes;
                    this.shouldRecord = true;
                }
            }

            public boolean add(long value) {
                if (!shouldRecord) {
                    return false;
                }
                instrument.add(value, finalAttributes);
                return true;
            }

            @Override
            public void close() {
                counters.remove(this);
            }
        }
    }

    public class ObservableLongCounterBundle implements Setup {
        private final List<ObservableLongCounter> counters = new CopyOnWriteArrayList<>();
        private final String name;
        private final String desc;
        private final String unit;

        private io.opentelemetry.api.metrics.ObservableLongCounter instrument;

        @SuppressWarnings("this-escape")
        public ObservableLongCounterBundle(String name, String desc, String unit) {
            this.name = name;
            this.desc = desc;
            this.unit = unit;
            waitingSetups.add(this);
            setup0();
        }

        public ObservableLongCounter register(MetricsLevel level, Attributes attributes,
            Consumer<ObservableLong> callback) {
            ObservableLongCounter counter = new ObservableLongCounter(level, attributes, callback);
            counters.add(counter);
            counter.setup();
            return counter;
        }

        public synchronized void setup() {
            counters.forEach(ObservableLongCounter::setup);
            this.instrument = meter.counterBuilder(name)
                .setDescription(desc)
                .setUnit(unit)
                .buildWithCallback(measurement -> counters.forEach(counter -> counter.record(measurement)));
        }

        public final class ObservableLongCounter implements AutoCloseable {
            private final MetricsLevel level;
            private final Attributes counterAttributes;
            private final Consumer<ObservableLong> callback;
            private final AtomicBoolean closed = new AtomicBoolean(false);
            private Attributes finalAttributes = Attributes.empty();
            private volatile boolean shouldRecord = true;

            private ObservableLongCounter(MetricsLevel level, Attributes attributes,
                Consumer<ObservableLong> callback) {
                this.level = level;
                this.counterAttributes = attributes;
                this.callback = callback;
                this.finalAttributes = attributes;
            }

            private void setup() {
                if (meter != null && globalConfig != null) {
                    this.finalAttributes = Attributes.builder()
                        .putAll(globalConfig.getBaseAttributes())
                        .putAll(counterAttributes)
                        .build();
                    this.shouldRecord = level.isWithin(globalConfig.getMetricsLevel());
                } else {
                    this.finalAttributes = counterAttributes;
                    this.shouldRecord = true;
                }
            }

            private void record(ObservableLongMeasurement measurement) {
                if (closed.get() || !shouldRecord) {
                    return;
                }
                try {
                    callback.accept(new ObservableLong(measurement, finalAttributes));
                } catch (Throwable ignored) {
                    // Skip one callback sample when the dynamic value supplier is temporarily unavailable.
                }
            }

            @Override
            public void close() {
                closed.set(true);
                counters.remove(this);
            }
        }

        public final class ObservableLong {
            private final ObservableLongMeasurement measurement;
            private final Attributes baseAttributes;

            private ObservableLong(ObservableLongMeasurement measurement, Attributes baseAttributes) {
                this.measurement = measurement;
                this.baseAttributes = baseAttributes;
            }

            public void record(long value) {
                measurement.record(value, baseAttributes);
            }

            public void record(long value, Attributes attributes) {
                measurement.record(value, Attributes.builder()
                    .putAll(baseAttributes)
                    .putAll(attributes)
                    .build());
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
            LongGauge gauge = new LongGauge(level, attributes, null);
            gauges.add(gauge);
            gauge.setup();
            return gauge;
        }

        public LongGauge register(MetricsLevel level, Attributes attributes,
            Consumer<ObservableLong> callback) {
            LongGauge gauge = new LongGauge(level, attributes, callback);
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
            private final AtomicReference<LongSupplier> valueGetter = new AtomicReference<>();
            private final Consumer<ObservableLong> callback;
            private Attributes finalAttributes = Attributes.empty();
            private volatile boolean shouldRecord = true;

            private LongGauge(MetricsLevel level, Attributes attributes,
                Consumer<ObservableLong> callback) {
                this.level = level;
                this.gaugeAttributes = attributes;
                this.callback = callback;
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
                valueGetter.set(null);
                value.set(newValue);
                hasValue.set(true);
            }

            public void record(LongSupplier valueGetter) {
                this.valueGetter.set(valueGetter);
                hasValue.set(false);
            }

            public void clear() {
                valueGetter.set(null);
                hasValue.set(false);
            }

            private void record(ObservableLongMeasurement measurement) {
                if (!shouldRecord) {
                    return;
                }
                if (callback != null) {
                    try {
                        callback.accept(new ObservableLong(measurement, finalAttributes));
                    } catch (Throwable ignored) {
                        // Skip one callback sample when the dynamic value supplier is temporarily unavailable.
                    }
                    return;
                }
                LongSupplier getter = valueGetter.get();
                if (getter != null) {
                    try {
                        measurement.record(getter.getAsLong(), finalAttributes);
                    } catch (Throwable ignored) {
                        // Skip one callback sample when the dynamic value supplier is temporarily unavailable.
                    }
                } else if (hasValue.get()) {
                    measurement.record(value.get(), finalAttributes);
                }
            }

            @Override
            public void close() {
                gauges.remove(this);
                hasValue.set(false);
            }
        }

        public final class ObservableLong {
            private final ObservableLongMeasurement measurement;
            private final Attributes baseAttributes;

            private ObservableLong(ObservableLongMeasurement measurement, Attributes baseAttributes) {
                this.measurement = measurement;
                this.baseAttributes = baseAttributes;
            }

            public void record(long value) {
                measurement.record(value, baseAttributes);
            }

            public void record(long value, Attributes attributes) {
                measurement.record(value, Attributes.builder()
                    .putAll(baseAttributes)
                    .putAll(attributes)
                    .build());
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
            DoubleGauge gauge = new DoubleGauge(level, attributes, null);
            gauges.add(gauge);
            gauge.setup();
            return gauge;
        }

        public DoubleGauge register(MetricsLevel level, Attributes attributes,
            Consumer<ObservableDouble> callback) {
            DoubleGauge gauge = new DoubleGauge(level, attributes, callback);
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
            private final AtomicReference<DoubleSupplier> valueGetter = new AtomicReference<>();
            private final Consumer<ObservableDouble> callback;
            private Attributes finalAttributes = Attributes.empty();
            private volatile boolean shouldRecord = true;

            private DoubleGauge(MetricsLevel level, Attributes attributes,
                Consumer<ObservableDouble> callback) {
                this.level = level;
                this.gaugeAttributes = attributes;
                this.callback = callback;
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
                valueGetter.set(null);
                value.set(newValue);
                hasValue.set(true);
            }

            public void record(DoubleSupplier valueGetter) {
                this.valueGetter.set(valueGetter);
                hasValue.set(false);
            }

            public void clear() {
                valueGetter.set(null);
                hasValue.set(false);
            }

            private void record(ObservableDoubleMeasurement measurement) {
                if (!shouldRecord) {
                    return;
                }
                if (callback != null) {
                    try {
                        callback.accept(new ObservableDouble(measurement, finalAttributes));
                    } catch (Throwable ignored) {
                        // Skip one callback sample when the dynamic value supplier is temporarily unavailable.
                    }
                    return;
                }
                DoubleSupplier getter = valueGetter.get();
                if (getter != null) {
                    try {
                        measurement.record(getter.getAsDouble(), finalAttributes);
                    } catch (Throwable ignored) {
                        // Skip one callback sample when the dynamic value supplier is temporarily unavailable.
                    }
                } else if (hasValue.get()) {
                    measurement.record(value.get(), finalAttributes);
                }
            }

            @Override
            public void close() {
                gauges.remove(this);
                hasValue.set(false);
            }
        }

        public final class ObservableDouble {
            private final ObservableDoubleMeasurement measurement;
            private final Attributes baseAttributes;

            private ObservableDouble(ObservableDoubleMeasurement measurement, Attributes baseAttributes) {
                this.measurement = measurement;
                this.baseAttributes = baseAttributes;
            }

            public void record(double value) {
                measurement.record(value, baseAttributes);
            }

            public void record(double value, Attributes attributes) {
                measurement.record(value, Attributes.builder()
                    .putAll(baseAttributes)
                    .putAll(attributes)
                    .build());
            }
        }
    }

    public class LazyLongCounter implements Setup, io.opentelemetry.api.metrics.LongCounter {
        private final Function<Meter, io.opentelemetry.api.metrics.LongCounter> newFunc;
        private io.opentelemetry.api.metrics.LongCounter counter = new NoopLongCounter();

        @SuppressWarnings("this-escape")
        public LazyLongCounter(Function<Meter, io.opentelemetry.api.metrics.LongCounter> newFunc) {
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
