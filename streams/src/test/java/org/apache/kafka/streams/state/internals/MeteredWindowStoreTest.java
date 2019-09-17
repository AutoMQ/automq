/*
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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static java.time.Instant.ofEpochMilli;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByNameFilterByTags;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MeteredWindowStoreTest {
    private InternalMockProcessorContext context;
    @SuppressWarnings("unchecked")
    private final WindowStore<Bytes, byte[]> innerStoreMock = createNiceMock(WindowStore.class);
    private final MeteredWindowStore<String, String> store = new MeteredWindowStore<>(
        innerStoreMock,
        10L, // any size
        "scope",
        new MockTime(),
        Serdes.String(),
        new SerdeThatDoesntHandleNull()
    );
    private final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));

    {
        expect(innerStoreMock.name()).andReturn("mocked-store").anyTimes();
    }

    @Before
    public void setUp() {
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test", StreamsConfig.METRICS_LATEST);

        context = new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            streamsMetrics,
            new StreamsConfig(StreamsTestUtils.getStreamsConfig()),
            NoOpRecordCollector::new,
            new ThreadCache(new LogContext("testCache "), 0, streamsMetrics)
        );
    }

    @Test
    public void testMetrics() {
        replay(innerStoreMock);
        store.init(context, store);
        final JmxReporter reporter = new JmxReporter("kafka.streams");
        metrics.addReporter(reporter);
        assertTrue(reporter.containsMbean(String.format("kafka.streams:type=stream-%s-state-metrics,client-id=%s,task-id=%s,%s-state-id=%s",
                "scope", "test", context.taskId().toString(), "scope", "mocked-store")));
        assertTrue(reporter.containsMbean(String.format("kafka.streams:type=stream-%s-state-metrics,client-id=%s,task-id=%s,%s-state-id=%s",
                "scope", "test", context.taskId().toString(), "scope", "all")));
    }

    @Test
    public void shouldRecordRestoreLatencyOnInit() {
        innerStoreMock.init(context, store);
        expectLastCall();
        replay(innerStoreMock);
        store.init(context, store);
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "restore-total", "stream-scope-state-metrics", singletonMap("scope-state-id", "all")).metricValue());
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "restore-total", "stream-scope-state-metrics", singletonMap("scope-state-id", "mocked-store")).metricValue());
    }

    @Test
    public void shouldRecordPutLatency() {
        final byte[] bytes = "a".getBytes();
        innerStoreMock.put(eq(Bytes.wrap(bytes)), anyObject(), eq(context.timestamp()));
        expectLastCall();
        replay(innerStoreMock);

        store.init(context, store);
        store.put("a", "a");
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "put-total", "stream-scope-state-metrics", singletonMap("scope-state-id", "all")).metricValue());
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "put-total", "stream-scope-state-metrics", singletonMap("scope-state-id", "mocked-store")).metricValue());
        verify(innerStoreMock);
    }

    @Test
    public void shouldRecordFetchLatency() {
        expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 1, 1)).andReturn(KeyValueIterators.<byte[]>emptyWindowStoreIterator());
        replay(innerStoreMock);

        store.init(context, store);
        store.fetch("a", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-state-metrics", singletonMap("scope-state-id", "all")).metricValue());
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-state-metrics", singletonMap("scope-state-id", "mocked-store")).metricValue());
        verify(innerStoreMock);
    }

    @Test
    public void shouldRecordFetchRangeLatency() {
        expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), Bytes.wrap("b".getBytes()), 1, 1)).andReturn(KeyValueIterators.<Windowed<Bytes>, byte[]>emptyIterator());
        replay(innerStoreMock);

        store.init(context, store);
        store.fetch("a", "b", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-state-metrics", singletonMap("scope-state-id", "all")).metricValue());
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-state-metrics", singletonMap("scope-state-id", "mocked-store")).metricValue());
        verify(innerStoreMock);
    }

    @Test
    public void shouldRecordFlushLatency() {
        innerStoreMock.flush();
        expectLastCall();
        replay(innerStoreMock);

        store.init(context, store);
        store.flush();
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "flush-total", "stream-scope-state-metrics", singletonMap("scope-state-id", "all")).metricValue());
        assertEquals(1.0, getMetricByNameFilterByTags(metrics, "flush-total", "stream-scope-state-metrics", singletonMap("scope-state-id", "mocked-store")).metricValue());
        verify(innerStoreMock);
    }

    @Test
    public void shouldCloseUnderlyingStore() {
        innerStoreMock.close();
        expectLastCall();
        replay(innerStoreMock);

        store.init(context, store);
        store.close();
        verify(innerStoreMock);
    }

    @Test
    public void shouldNotThrowNullPointerExceptionIfFetchReturnsNull() {
        expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 0)).andReturn(null);
        replay(innerStoreMock);

        store.init(context, store);
        assertNull(store.fetch("a", 0));
    }

    private interface CachedWindowStore extends WindowStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSetFlushListenerOnWrappedCachingStore() {
        final CachedWindowStore cachedWindowStore = mock(CachedWindowStore.class);

        expect(cachedWindowStore.setFlushListener(anyObject(CacheFlushListener.class), eq(false))).andReturn(true);
        replay(cachedWindowStore);

        final MeteredWindowStore<String, String> metered = new MeteredWindowStore<>(
            cachedWindowStore,
            10L, // any size
            "scope",
            new MockTime(),
            Serdes.String(),
            new SerdeThatDoesntHandleNull()
        );
        assertTrue(metered.setFlushListener(null, false));

        verify(cachedWindowStore);
    }

    @Test
    public void shouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        assertFalse(store.setFlushListener(null, false));
    }

}
