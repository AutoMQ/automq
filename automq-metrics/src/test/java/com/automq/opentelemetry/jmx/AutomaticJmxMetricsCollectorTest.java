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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.SdkMeterProviderUtil;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class AutomaticJmxMetricsCollectorTest {
    private final MBeanServer server = MBeanServerFactory.newMBeanServer();
    private final InMemoryMetricReader first = InMemoryMetricReader.create();
    private final InMemoryMetricReader second = InMemoryMetricReader.create();
    private final SdkMeterProvider provider = SdkMeterProvider.builder().registerMetricReader(first).registerMetricReader(second).build();
    private AutomaticJmxMetricsCollector collector;
    private final List<MutableBean> beans = new ArrayList<>();

    @AfterEach
    void tearDown() {
        beans.forEach(bean -> bean.release.countDown());
        if (collector != null) {
            collector.close();
        }
        provider.close();
    }

    /** Both readers consume the same background sample, preserving native and worker identity. */
    @Test
    void testSharedSnapshotAndUnknownDomain() throws Exception {
        MutableBean bean = register("unknown.vendor:type=source,connector=mysql,task=7", Map.of("Value", 37L, "Connected", true, "Text", "sensitive text"));
        start(10000, limits(2, 50, 100, 30000, 1000, 100));
        collector.start();
        await(() -> points(first, "unknown.vendor").size() == 2);
        int reads = bean.reads.get();
        List<DoublePointData> points = points(second, "unknown.vendor");
        assertEquals(2, points.size());
        DoublePointData value = point(points, "Value");
        assertEquals(37, value.getValue());
        assertEquals("mysql", value.getAttributes().get(AttributeKey.stringKey("jmx_prop_connector")));
        assertEquals("7", value.getAttributes().get(AttributeKey.stringKey("jmx_prop_task")));
        assertEquals("worker-1", value.getAttributes().get(AttributeKey.stringKey("node_id")));
        assertEquals("unknown.vendor:connector=mysql,task=7,type=source",
            value.getAttributes().get(AttributeKey.stringKey("jmx_object_name")));
        assertEquals(1, point(points, "Connected").getValue());
        assertEquals(reads, bean.reads.get(), "OTel collection must never invoke JMX getters");
        assertTrue(points.stream().noneMatch(data -> data.getAttributes().toString().contains("sensitive text")));
        collector.close();
        collector.close();
        collector.start();
        assertTrue(points(first, "unknown.vendor").isEmpty());
    }

    /** New MBeans, changing attributes and a replacement at the same ObjectName need no rules. */
    @Test
    void testRegistrationRemovalReplacementAndDynamicAttributes() throws Exception {
        start(10, limits(2, 50, 100, 100, 1000, 100));
        MutableBean original = register("new.connector:type=source,name=one", Map.of("Value", 1L));
        await(() -> points(first, "new.connector").size() == 1);
        original.values.put("Added", 2);
        await(() -> points(first, "new.connector").size() == 2);
        original.values.remove("Value");
        await(() -> points(first, "new.connector").size() == 1
            && "Added".equals(points(first, "new.connector").get(0).getAttributes().get(AttributeKey.stringKey("jmx_attribute"))));
        server.unregisterMBean(new ObjectName("new.connector:type=source,name=one"));
        await(() -> points(first, "new.connector").isEmpty());
        register("new.connector:name=one,type=source", Map.of("Value", 99));
        await(() -> points(first, "new.connector").size() == 1 && points(first, "new.connector").get(0).getValue() == 99);
    }

    /** A getter that ignores interruption cannot block readers, and its cached sample expires. */
    @Test
    void testSlowGetterIsolationAndStaleSamples() throws Exception {
        MutableBean slow = register("slow.vendor:type=source", Map.of("Value", 5));
        MutableBean healthy = register("healthy.vendor:type=source", Map.of("Value", 7));
        start(10, limits(2, 30, 70, 70, 1000, 100));
        await(() -> points(first, "slow.vendor").size() == 1);
        slow.block = true;
        assertTrue(slow.entered.await(2, TimeUnit.SECONDS));
        await(() -> points(first, "slow.vendor").isEmpty());
        await(() -> points(first, "healthy.vendor").size() == 1);
        // More than four scan rounds must not let one stuck MBean consume every reader thread.
        Thread.sleep(200);
        healthy.values.put("Value", 9);
        await(() -> !points(first, "healthy.vendor").isEmpty() && points(first, "healthy.vendor").get(0).getValue() == 9);
        long start = System.nanoTime();
        assertEquals(9, point(points(second, "healthy.vendor"), "Value").getValue());
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 500);
        await(() -> counter("connect_jmx_collector_timeouts_total") > 0);
    }

    /** Discovery rotates beyond a budget-limited prefix instead of starving later MBeans. */
    @Test
    void testBudgetLimitedDiscoveryIsFair() throws Exception {
        for (int index = 0; index < 8; index++) {
            MutableBean bean = register("a.slow:type=source,id=" + index, Map.of("Value", index));
            bean.delayMs = 12;
        }
        register("z.last:type=source", Map.of("Value", 42));
        start(5, limits(1, 30, 20, 200, 1000, 100));
        await(() -> points(first, "z.last").size() == 1);
        assertEquals(42, point(points(first, "z.last"), "Value").getValue());
    }

    /** Point/family limits remain global when many MBeans each expose structured values. */
    @Test
    void testGlobalBudgetsAndLongIdentities() throws Exception {
        for (int index = 0; index < 8; index++) {
            register("bounded.vendor:type=source,id=" + index, Map.of("Values", new long[] {1, 2, 3, 4, 5, 6}));
        }
        register("huge.vendor:type=source,name=" + "x".repeat(1100), Map.of("Value", 1));
        start(10, limits(2, 30, 100, 200, 5, 3));
        await(() -> !points(first, "bounded.vendor").isEmpty());
        assertTrue(points(first, "bounded.vendor").size() <= 5);
        assertTrue(points(first, "huge.vendor").isEmpty());
        await(() -> counter("connect_jmx_collector_dropped_total") > 0);
    }

    /** A tight SDK stream limit must drop source points observably rather than emit anonymous overflow gauges. */
    @Test
    void testSourceBudgetRespectsSdkCardinalityLimit() throws Exception {
        for (int index = 0; index < 6; index++) {
            register("cardinality.vendor:type=source,id=" + index, Map.of("Value", index));
        }
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        SdkMeterProviderBuilder builder = SdkMeterProvider.builder();
        SdkMeterProviderUtil.registerMetricReaderWithCardinalitySelector(builder, reader, instrument -> 3);
        try (SdkMeterProvider limitedProvider = builder.build();
            AutomaticJmxMetricsCollector limitedCollector = new AutomaticJmxMetricsCollector(
                limitedProvider.get("limited-jmx"), 10, Attributes.empty(), server,
                AutomaticJmxMetricsCollector.Limits.defaults(10, 3))) {
            limitedCollector.start();
            await(() -> !points(reader, "cardinality.vendor").isEmpty());
            assertTrue(points(reader, "cardinality.vendor").size() <= 2);
            assertTrue(reader.collectAllMetrics().stream()
                .filter(metric -> metric.getName().endsWith("_value"))
                .flatMap(metric -> metric.getDoubleGaugeData().getPoints().stream())
                .noneMatch(point -> Boolean.TRUE.equals(point.getAttributes().get(AttributeKey.booleanKey("otel.metric.overflow")))));
        }
    }

    /** The sampler bounds retained points before publication, across all MBeans rather than per bean. */
    @Test
    void testSamplerCacheHasGlobalPointBudget() throws Exception {
        for (int index = 0; index < 8; index++) {
            register("cache.vendor:type=source,id=" + index, Map.of("Values", new int[] {1, 2, 3, 4, 5, 6}));
        }
        try (JmxMetricSampler sampler = new JmxMetricSampler(server, Attributes.empty(), limits(2, 30, 200, 1000, 5, 100))) {
            sampler.start();
            for (int iteration = 0; iteration < 3; iteration++) {
                int retained = sampler.sample().stream().mapToInt(sample -> sample.points.size()).sum();
                assertTrue(retained > 0 && retained <= 5);
            }
            assertTrue(sampler.stats.dropped.get() > 0);
        }
    }

    /** Existing families do not consume the budget twice, and removed callbacks can be recreated. */
    @Test
    void testFamilyBudgetAndCallbackRecreation() throws Exception {
        MutableBean bean = register("families.vendor:type=source", Map.of("A", 1, "B", 2));
        start(10, limits(2, 30, 100, 200, 100, 3));
        await(() -> points(first, "families.vendor").size() == 2);
        bean.values.put("C", 3);
        await(() -> points(first, "families.vendor").size() == 3);
        bean.values.clear();
        await(() -> points(first, "families.vendor").isEmpty());
        bean.values.put("A", 4);
        await(() -> points(first, "families.vendor").size() == 1);
        assertEquals(4, point(points(first, "families.vendor"), "A").getValue());
    }

    /** Replacing a hung MBean permits its new generation to be sampled without accepting old data. */
    @Test
    void testReplacementRecoversFromHungPreviousGeneration() throws Exception {
        String objectName = "restart.vendor:type=source";
        MutableBean original = register(objectName, Map.of("Value", 1));
        start(10, limits(2, 30, 70, 100, 1000, 100));
        await(() -> points(first, "restart.vendor").size() == 1);
        original.block = true;
        assertTrue(original.entered.await(2, TimeUnit.SECONDS));
        server.unregisterMBean(new ObjectName(objectName));
        register(objectName, Map.of("Value", 2));
        await(() -> !points(first, "restart.vendor").isEmpty() && points(first, "restart.vendor").get(0).getValue() == 2);
        original.release.countDown();
        Thread.sleep(50);
        assertEquals(2, point(points(first, "restart.vendor"), "Value").getValue());
    }

    private MutableBean register(String name, Map<String, Object> values) throws Exception {
        MutableBean bean = new MutableBean(values);
        server.registerMBean(bean, new ObjectName(name));
        beans.add(bean);
        return bean;
    }

    private void start(long interval, AutomaticJmxMetricsCollector.Limits limits) {
        collector = new AutomaticJmxMetricsCollector(provider.get("jmx-test"), interval,
            Attributes.of(AttributeKey.stringKey("node_id"), "worker-1"), server, limits);
        collector.start();
    }

    private static AutomaticJmxMetricsCollector.Limits limits(int threads, long readTimeout, long scanBudget,
        long stale, int series, int families) {
        return new AutomaticJmxMetricsCollector.Limits(threads, readTimeout, scanBudget, stale, 100, 100, series, families, 8, 64);
    }

    private long counter(String name) {
        return first.collectAllMetrics().stream().filter(metric -> metric.getName().equals(name))
            .flatMap(metric -> metric.getLongSumData().getPoints().stream()).mapToLong(point -> point.getValue()).sum();
    }

    private static List<DoublePointData> points(InMemoryMetricReader reader, String domain) {
        List<DoublePointData> result = new ArrayList<>();
        for (MetricData metric : reader.collectAllMetrics()) {
            if (metric.getName().startsWith("connect_jmx_") && metric.getName().endsWith("_value")) {
                metric.getDoubleGaugeData().getPoints().stream()
                    .filter(point -> domain.equals(point.getAttributes().get(AttributeKey.stringKey("jmx_domain"))))
                    .forEach(result::add);
            }
        }
        return result;
    }

    private static DoublePointData point(List<DoublePointData> points, String attribute) {
        return points.stream().filter(point -> attribute.equals(point.getAttributes().get(AttributeKey.stringKey("jmx_attribute"))))
            .findFirst().orElseThrow();
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(condition.getAsBoolean(), "Condition was not satisfied before timeout");
    }

    private static final class MutableBean implements DynamicMBean {
        private final Map<String, Object> values = new ConcurrentHashMap<>();
        private final AtomicInteger reads = new AtomicInteger();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile boolean block;
        private volatile long delayMs;

        private MutableBean(Map<String, Object> values) {
            this.values.putAll(values);
        }

        @Override
        public Object getAttribute(String attribute) throws AttributeNotFoundException {
            if (!values.containsKey(attribute)) {
                throw new AttributeNotFoundException(attribute);
            }
            return values.get(attribute);
        }

        @Override
        public AttributeList getAttributes(String[] attributes) {
            reads.incrementAndGet();
            if (block) {
                entered.countDown();
                boolean interrupted = false;
                while (release.getCount() > 0) {
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            if (delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            AttributeList result = new AttributeList();
            for (String name : attributes) {
                Object value = values.get(name);
                if (value != null) {
                    result.add(new Attribute(name, value));
                }
            }
            return result;
        }

        @Override
        public MBeanInfo getMBeanInfo() {
            MBeanAttributeInfo[] attributes = values.entrySet().stream()
                .map(entry -> new MBeanAttributeInfo(entry.getKey(), entry.getValue().getClass().getName(), "Test attribute", true, false, false))
                .toArray(MBeanAttributeInfo[]::new);
            return new MBeanInfo(getClass().getName(), "Mutable test MBean", attributes, null, null, null);
        }

        @Override
        public void setAttribute(Attribute attribute) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AttributeList setAttributes(AttributeList attributes) {
            return new AttributeList();
        }

        @Override
        public Object invoke(String actionName, Object[] parameters, String[] signature) {
            throw new UnsupportedOperationException();
        }
    }
}
