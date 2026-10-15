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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;

final class JmxMetricSampler implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxMetricSampler.class);
    private final MBeanServer server;
    private final Attributes baseAttributes;
    private final AutomaticJmxMetricsCollector.Limits limits;
    private final ThreadPoolExecutor readers;
    private final Map<ObjectName, Long> generations = new ConcurrentHashMap<>();
    private final Map<ObjectName, Long> inFlight = new ConcurrentHashMap<>();
    private final AtomicLong generation = new AtomicLong();
    private final Map<ObjectName, BeanSample> samples = new HashMap<>();
    private final NotificationListener listener = (notification, handback) -> {
        if (notification instanceof MBeanServerNotification) {
            generations.put(((MBeanServerNotification) notification).getMBeanName(), generation.incrementAndGet());
        }
    };
    final Stats stats = new Stats();
    private boolean listening;
    private int cursor;
    private int cachedPoints;

    JmxMetricSampler(MBeanServer server, Attributes baseAttributes, AutomaticJmxMetricsCollector.Limits limits) {
        this.server = server;
        this.baseAttributes = baseAttributes;
        this.limits = limits;
        AtomicLong threadId = new AtomicLong();
        readers = new ThreadPoolExecutor(limits.readThreads, limits.readThreads, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(limits.readThreads), runnable -> {
                Thread thread = new Thread(runnable, "connect-jmx-reader-" + threadId.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            });
    }

    void start() {
        try {
            server.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, listener, null, null);
            listening = true;
        } catch (Exception e) {
            // Periodic full discovery still works when a server does not support notifications.
            LOGGER.debug("JMX lifecycle notifications unavailable", e);
        }
    }

    List<BeanSample> sample() throws InterruptedException {
        long start = System.nanoTime();
        Set<ObjectName> live = server.queryNames(null, null);
        List<ObjectName> names = new ArrayList<>(live);
        names.sort(Comparator.comparing(ObjectName::getCanonicalName));
        int count = Math.min(names.size(), limits.maxBeans);
        if (!names.isEmpty()) {
            java.util.Collections.rotate(names, -(cursor % names.size()));
        }
        stats.dropped.addAndGet(names.size() - count);
        int visited = collect(names.subList(0, count), start + TimeUnit.MILLISECONDS.toNanos(limits.scanBudgetMs));
        if (!names.isEmpty()) {
            cursor = (cursor + Math.max(1, visited)) % names.size();
        }
        long now = System.nanoTime();
        samples.entrySet().removeIf(entry -> !live.contains(entry.getKey())
            || entry.getValue().generation != generations.getOrDefault(entry.getKey(), 0L)
            || now - entry.getValue().sampledAt > TimeUnit.MILLISECONDS.toNanos(limits.staleMs));
        cachedPoints = samples.values().stream().mapToInt(sample -> sample.points.size()).sum();
        generations.keySet().retainAll(live);
        stats.scanDurationMs.set(TimeUnit.NANOSECONDS.toMillis(now - start));
        return List.copyOf(samples.values());
    }

    private int collect(List<ObjectName> names, long deadline) throws InterruptedException {
        BlockingQueue<Future<BeanSample>> completions = new LinkedBlockingQueue<>();
        Map<Future<BeanSample>, Pending> pending = new LinkedHashMap<>();
        int submitted = 0;
        try {
            while ((submitted < names.size() || !pending.isEmpty()) && System.nanoTime() < deadline) {
                while (submitted < names.size() && pending.size() < limits.readThreads) {
                    ObjectName name = names.get(submitted++);
                    long epoch = generations.getOrDefault(name, 0L);
                    if (!acquire(name, epoch)) {
                        stats.dropped.incrementAndGet();
                        continue;
                    }
                    try {
                        ReadFuture future = new ReadFuture(name, epoch, completions);
                        readers.execute(future);
                        pending.put(future, new Pending(name, epoch, System.nanoTime()));
                    } catch (RejectedExecutionException e) {
                        inFlight.remove(name, epoch);
                        stats.dropped.addAndGet(1L + names.size() - submitted);
                        submitted = names.size();
                        break;
                    }
                }
                if (pending.isEmpty()) {
                    break;
                }
                long waitUntil = deadline;
                for (Pending read : pending.values()) {
                    waitUntil = Math.min(waitUntil, read.startedAt + TimeUnit.MILLISECONDS.toNanos(limits.readTimeoutMs));
                }
                Future<BeanSample> completed = completions.poll(Math.max(0, waitUntil - System.nanoTime()), TimeUnit.NANOSECONDS);
                if (completed != null) {
                    Pending read = pending.remove(completed);
                    accept(completed, read);
                }
                expire(pending, System.nanoTime());
            }
        } finally {
            stats.dropped.addAndGet(names.size() - submitted);
            for (Future<BeanSample> future : pending.keySet()) {
                future.cancel(true);
                stats.timeouts.incrementAndGet();
            }
        }
        return submitted;
    }

    private boolean acquire(ObjectName name, long epoch) {
        while (true) {
            Long previous = inFlight.get(name);
            if (previous != null && previous == epoch) {
                return false;
            }
            if (previous == null ? inFlight.putIfAbsent(name, epoch) == null : inFlight.replace(name, previous, epoch)) {
                return true;
            }
        }
    }

    private void accept(Future<BeanSample> completed, Pending read) {
        if (read == null || read.generation != generations.getOrDefault(read.name, 0L)) {
            return;
        }
        try {
            BeanSample sample = completed.get();
            if (sample != null) {
                cache(read.name, sample);
                stats.reads.incrementAndGet();
            }
        } catch (CancellationException e) {
            // Timed-out reads may complete after their result has been discarded.
        } catch (Exception e) {
            stats.readErrors.incrementAndGet();
            LOGGER.debug("Failed to collect JMX attributes", e);
        }
    }

    private void cache(ObjectName name, BeanSample sample) {
        BeanSample previous = samples.remove(name);
        if (previous != null) {
            cachedPoints -= previous.points.size();
        }
        if (sample.points.isEmpty()) {
            return;
        }
        while (cachedPoints + sample.points.size() > limits.maxSeries && !samples.isEmpty()) {
            Map.Entry<ObjectName, BeanSample> oldest = samples.entrySet().stream()
                .min(Comparator.comparingLong(entry -> entry.getValue().sampledAt)).orElseThrow();
            int removed = oldest.getValue().points.size();
            samples.remove(oldest.getKey());
            cachedPoints -= removed;
            stats.dropped.addAndGet(removed);
        }
        samples.put(name, sample);
        cachedPoints += sample.points.size();
    }

    private void expire(Map<Future<BeanSample>, Pending> pending, long now) {
        Iterator<Map.Entry<Future<BeanSample>, Pending>> iterator = pending.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Future<BeanSample>, Pending> entry = iterator.next();
            if (now - entry.getValue().startedAt >= TimeUnit.MILLISECONDS.toNanos(limits.readTimeoutMs)) {
                entry.getKey().cancel(true);
                iterator.remove();
                stats.timeouts.incrementAndGet();
            }
        }
    }

    private BeanSample read(ObjectName name, long epoch) throws Exception {
        try {
            if (!validIdentity(name)) {
                stats.dropped.incrementAndGet();
                return new BeanSample(name.getDomain(), epoch, System.nanoTime(), List.of());
            }
            List<String> readable = new ArrayList<>();
            for (MBeanAttributeInfo attribute : server.getMBeanInfo(name).getAttributes()) {
                if (attribute.isReadable()) {
                    if (attribute.getName().length() > JmxMetricValueFlattener.MAX_LABEL_LENGTH) {
                        stats.dropped.incrementAndGet();
                        continue;
                    }
                    if (readable.size() >= limits.maxAttributes) {
                        stats.dropped.incrementAndGet();
                        break;
                    }
                    readable.add(attribute.getName());
                }
            }
            AttributesBuilder attributes = baseAttributes.toBuilder().put("jmx_domain", name.getDomain())
                .put("jmx_object_name", name.getCanonicalName());
            name.getKeyPropertyList().forEach((key, value) -> attributes.put("jmx_prop_" + JmxMetricNaming.labelPart(key), value));
            JmxMetricValueFlattener flattener = new JmxMetricValueFlattener(limits.maxDepth, limits.maxElements,
                Math.min(limits.maxSeries, limits.maxElements));
            AttributeList values = server.getAttributes(name, readable.toArray(new String[0]));
            stats.readErrors.addAndGet(Math.max(0, readable.size() - values.size()));
            for (Attribute value : values.asList()) {
                flattener.flatten(JmxMetricNaming.pathPart(value.getName()), value.getValue(), attributes.build());
            }
            stats.dropped.addAndGet(flattener.dropped());
            stats.unsupported.addAndGet(flattener.unsupported());
            return new BeanSample(name.getDomain(), epoch, System.nanoTime(), flattener.points());
        } catch (InstanceNotFoundException e) {
            // Discovery and attribute reads race normally with connector shutdown/restart.
            return null;
        }
    }

    private boolean validIdentity(ObjectName name) {
        if (name.getCanonicalName().length() > JmxMetricValueFlattener.MAX_IDENTITY_LENGTH) {
            return false;
        }
        return name.getKeyPropertyList().entrySet().stream().allMatch(entry ->
            entry.getKey().length() <= JmxMetricValueFlattener.MAX_LABEL_LENGTH
                && entry.getValue().length() <= JmxMetricValueFlattener.MAX_LABEL_LENGTH);
    }

    @Override
    public void close() {
        readers.shutdownNow();
        if (listening) {
            try {
                server.removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, listener);
            } catch (Exception e) {
                LOGGER.debug("Failed to remove JMX lifecycle listener", e);
            }
            listening = false;
        }
    }

    static final class BeanSample {
        final String domain;
        final long generation;
        final long sampledAt;
        final List<JmxMetricValueFlattener.Point> points;

        BeanSample(String domain, long generation, long sampledAt, List<JmxMetricValueFlattener.Point> points) {
            this.domain = domain;
            this.generation = generation;
            this.sampledAt = sampledAt;
            this.points = points;
        }
    }

    private static final class Pending {
        final ObjectName name;
        final long generation;
        final long startedAt;

        Pending(ObjectName name, long generation, long startedAt) {
            this.name = name;
            this.generation = generation;
            this.startedAt = startedAt;
        }
    }

    private final class ReadFuture extends FutureTask<BeanSample> {
        private final ObjectName name;
        private final long generation;
        private final BlockingQueue<Future<BeanSample>> completions;

        private ReadFuture(ObjectName name, long epoch, BlockingQueue<Future<BeanSample>> completions) {
            super(() -> read(name, epoch));
            this.name = name;
            this.generation = epoch;
            this.completions = completions;
        }

        @Override
        public void run() {
            try {
                super.run();
            } finally {
                // Cancellation does not stop arbitrary getters. Release only when the worker exits.
                inFlight.remove(name, generation);
            }
        }

        @Override
        protected void done() {
            completions.add(this);
        }
    }

    static final class Stats {
        final AtomicLong reads = new AtomicLong();
        final AtomicLong readErrors = new AtomicLong();
        final AtomicLong timeouts = new AtomicLong();
        final AtomicLong dropped = new AtomicLong();
        final AtomicLong unsupported = new AtomicLong();
        final AtomicLong scanDurationMs = new AtomicLong();
    }
}
