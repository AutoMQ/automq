/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.opentelemetry.jmx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;

/**
 * Discovers readable numeric JMX attributes without connector-specific configuration and records them
 * through an existing OpenTelemetry meter. Getters run only in a bounded background pool; OTel
 * callbacks consume immutable, expiring snapshots. Unknown types, units and counter semantics are
 * deliberately not inferred. This collector owns its threads, listeners and callback registrations.
 */
public final class AutomaticJmxMetricsCollector implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutomaticJmxMetricsCollector.class);
    private final Meter meter;
    private final long intervalMs;
    private final Attributes baseAttributes;
    private final Limits limits;
    private final JmxMetricSampler sampler;
    private final ScheduledExecutorService scheduler;
    private final Map<String, Family> families = new HashMap<>();
    // SDK instrument descriptors are not removable; bound distinct names across this lifecycle.
    private final Set<String> knownFamilies = new HashSet<>();
    private final List<AutoCloseable> monitoring = new ArrayList<>();
    private final AtomicLong activeSeries = new AtomicLong();
    private final AtomicLong activeFamilies = new AtomicLong();
    private final AtomicLong knownFamilyCount = new AtomicLong();
    private volatile boolean closed;
    private boolean started;

    /**
     * Creates an inactive collector for the platform MBean server. The interval must be positive;
     * base attributes identify the worker and are attached to every exported series.
     */
    public AutomaticJmxMetricsCollector(Meter meter, long intervalMs, Attributes baseAttributes) {
        this(meter, intervalMs, baseAttributes, ManagementFactory.getPlatformMBeanServer(), Limits.defaults(intervalMs));
    }

    /**
     * Creates a platform collector whose point budget also respects the owner's SDK cardinality limit.
     * No connector-specific configuration is required.
     */
    public AutomaticJmxMetricsCollector(Meter meter, long intervalMs, Attributes baseAttributes, int cardinalityLimit) {
        this(meter, intervalMs, baseAttributes, ManagementFactory.getPlatformMBeanServer(),
            Limits.defaults(intervalMs, cardinalityLimit));
    }

    AutomaticJmxMetricsCollector(Meter meter, long intervalMs, Attributes baseAttributes, MBeanServer server, Limits limits) {
        this.meter = Objects.requireNonNull(meter);
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("JMX collection interval must be positive");
        }
        this.intervalMs = intervalMs;
        this.baseAttributes = Objects.requireNonNull(baseAttributes);
        this.limits = limits;
        this.sampler = new JmxMetricSampler(server, baseAttributes, limits);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "connect-jmx-sampler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Starts background discovery once. Repeated calls, including calls after close, are harmless. */
    public synchronized void start() {
        if (started || closed) {
            return;
        }
        started = true;
        monitorCounter("connect_jmx_collector_reads_total", sampler.stats.reads);
        monitorCounter("connect_jmx_collector_read_errors_total", sampler.stats.readErrors);
        monitorCounter("connect_jmx_collector_timeouts_total", sampler.stats.timeouts);
        monitorCounter("connect_jmx_collector_dropped_total", sampler.stats.dropped);
        monitorCounter("connect_jmx_collector_unsupported_total", sampler.stats.unsupported);
        monitorGauge("connect_jmx_collector_active_series", activeSeries);
        monitorGauge("connect_jmx_collector_active_families", activeFamilies);
        monitorGauge("connect_jmx_collector_known_families", knownFamilyCount);
        monitorGauge("connect_jmx_collector_scan_duration_ms", sampler.stats.scanDurationMs);
        sampler.start();
        scheduler.scheduleWithFixedDelay(this::sample, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void monitorCounter(String name, AtomicLong value) {
        ObservableLongCounter counter = meter.counterBuilder(name).buildWithCallback(measurement -> measurement.record(value.get(), baseAttributes));
        monitoring.add(counter);
    }

    private void monitorGauge(String name, AtomicLong value) {
        ObservableLongGauge gauge = meter.gaugeBuilder(name).ofLongs().buildWithCallback(measurement -> measurement.record(value.get(), baseAttributes));
        monitoring.add(gauge);
    }

    private void sample() {
        try {
            publish(sampler.sample());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            sampler.stats.readErrors.incrementAndGet();
            LOGGER.warn("Failed to sample JMX metrics", e);
        }
    }

    private synchronized void publish(List<JmxMetricSampler.BeanSample> beans) {
        if (closed) {
            return;
        }
        Map<String, List<Sample>> next = new HashMap<>();
        int count = 0;
        Set<String> newFamilies = new HashSet<>();
        for (JmxMetricSampler.BeanSample bean : beans) {
            for (JmxMetricValueFlattener.Point point : bean.points) {
                if (count >= limits.maxSeries) {
                    sampler.stats.dropped.incrementAndGet();
                    continue;
                }
                String name = JmxMetricNaming.metricName(bean.domain, point.path);
                if (!knownFamilies.contains(name) && !newFamilies.contains(name)) {
                    if (knownFamilies.size() + newFamilies.size() >= limits.maxFamilies) {
                        sampler.stats.dropped.incrementAndGet();
                        continue;
                    }
                    newFamilies.add(name);
                }
                next.computeIfAbsent(name, ignored -> new ArrayList<>()).add(new Sample(point, bean.sampledAt));
                count++;
            }
        }
        // Remove callbacks for disappeared families, but retain their names for the lifetime budget.
        Iterator<Map.Entry<String, Family>> iterator = families.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Family> entry = iterator.next();
            if (!next.containsKey(entry.getKey())) {
                entry.getValue().samples = List.of();
                entry.getValue().gauge.close();
                iterator.remove();
            }
        }
        knownFamilies.addAll(newFamilies);
        for (Map.Entry<String, List<Sample>> entry : next.entrySet()) {
            Family family = families.get(entry.getKey());
            if (family == null) {
                family = new Family(entry.getKey());
                families.put(entry.getKey(), family);
            }
            family.samples = List.copyOf(entry.getValue());
        }
        activeSeries.set(count);
        activeFamilies.set(families.size());
        knownFamilyCount.set(knownFamilies.size());
    }

    /** Stops scheduling and reads, removes listeners and callbacks, and can be called repeatedly. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        scheduler.shutdownNow();
        sampler.close();
        families.values().forEach(family -> {
            family.samples = List.of();
            family.gauge.close();
        });
        families.clear();
        knownFamilies.clear();
        monitoring.forEach(callback -> {
            try {
                callback.close();
            } catch (Exception e) {
                LOGGER.debug("Failed to close JMX metric callback", e);
            }
        });
        monitoring.clear();
        activeSeries.set(0);
        activeFamilies.set(0);
        knownFamilyCount.set(0);
    }

    private final class Family {
        private volatile List<Sample> samples = List.of();
        private final ObservableDoubleGauge gauge;

        private Family(String name) {
            gauge = meter.gaugeBuilder(name).setDescription("Raw numeric JMX attribute value")
                .buildWithCallback(measurement -> {
                    long now = System.nanoTime();
                    for (Sample sample : samples) {
                        if (!closed && now - sample.sampledAt <= TimeUnit.MILLISECONDS.toNanos(limits.staleMs)) {
                            measurement.record(sample.point.value, sample.point.attributes);
                        }
                    }
                });
        }
    }

    private static final class Sample {
        final JmxMetricValueFlattener.Point point;
        final long sampledAt;

        Sample(JmxMetricValueFlattener.Point point, long sampledAt) {
            this.point = point;
            this.sampledAt = sampledAt;
        }
    }

    static final class Limits {
        final int readThreads;
        final long readTimeoutMs;
        final long scanBudgetMs;
        final long staleMs;
        final int maxBeans;
        final int maxAttributes;
        final int maxSeries;
        final int maxFamilies;
        final int maxDepth;
        final int maxElements;

        Limits(int readThreads, long readTimeoutMs, long scanBudgetMs, long staleMs, int maxBeans,
            int maxAttributes, int maxSeries, int maxFamilies, int maxDepth, int maxElements) {
            this.readThreads = readThreads;
            this.readTimeoutMs = readTimeoutMs;
            this.scanBudgetMs = scanBudgetMs;
            this.staleMs = staleMs;
            this.maxBeans = maxBeans;
            this.maxAttributes = maxAttributes;
            this.maxSeries = maxSeries;
            this.maxFamilies = maxFamilies;
            this.maxDepth = maxDepth;
            this.maxElements = maxElements;
        }

        static Limits defaults(long intervalMs) {
            long interval = Math.max(1, Math.min(intervalMs, TimeUnit.DAYS.toMillis(1)));
            return new Limits(4, 100, Math.max(100, Math.min(2000, interval)),
                Math.max(1000, interval * 3), 10000, 256, 20000, 4096, 8, 1024);
        }

        static Limits defaults(long intervalMs, int cardinalityLimit) {
            if (cardinalityLimit <= 0) {
                throw new IllegalArgumentException("Metric cardinality limit must be positive");
            }
            Limits defaults = defaults(intervalMs);
            return new Limits(defaults.readThreads, defaults.readTimeoutMs, defaults.scanBudgetMs, defaults.staleMs,
                defaults.maxBeans, defaults.maxAttributes, Math.min(defaults.maxSeries, Math.max(1, cardinalityLimit - 1)),
                defaults.maxFamilies, defaults.maxDepth, defaults.maxElements);
        }
    }
}
