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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

/**
 * A Metered {@link KeyValueStore} wrapper that is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
 * The inner {@link KeyValueStore} of this class is of type &lt;Bytes,byte[]&gt;, hence we use {@link Serde}s
 * to convert from &lt;K,V&gt; to &lt;Bytes,byte[]&gt;
 * @param <K>
 * @param <V>
 */
public class MeteredKeyValueStore<K, V>
    extends WrappedStateStore<KeyValueStore<Bytes, byte[]>, K, V>
    implements KeyValueStore<K, V> {

    final Serde<K> keySerde;
    final Serde<V> valueSerde;
    StateSerdes<K, V> serdes;

    private final String metricsScope;
    protected final Time time;
    protected Sensor putSensor;
    private Sensor putIfAbsentSensor;
    protected Sensor getSensor;
    private Sensor deleteSensor;
    private Sensor putAllSensor;
    private Sensor allSensor;
    private Sensor rangeSensor;
    private Sensor flushSensor;
    private StreamsMetricsImpl streamsMetrics;
    private final String threadId;
    private String taskId;

    MeteredKeyValueStore(final KeyValueStore<Bytes, byte[]> inner,
                         final String metricsScope,
                         final Time time,
                         final Serde<K> keySerde,
                         final Serde<V> valueSerde) {
        super(inner);
        this.metricsScope = metricsScope;
        threadId = Thread.currentThread().getName();
        this.time = time != null ? time : Time.SYSTEM;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        taskId = context.taskId().toString();
        initStoreSerde(context);
        streamsMetrics = (StreamsMetricsImpl) context.metrics();

        putSensor = StateStoreMetrics.putSensor(threadId, taskId, metricsScope, name(), streamsMetrics);
        putIfAbsentSensor = StateStoreMetrics.putIfAbsentSensor(threadId, taskId, metricsScope, name(), streamsMetrics);
        putAllSensor = StateStoreMetrics.putAllSensor(threadId, taskId, metricsScope, name(), streamsMetrics);
        getSensor = StateStoreMetrics.getSensor(threadId, taskId, metricsScope, name(), streamsMetrics);
        allSensor = StateStoreMetrics.allSensor(threadId, taskId, metricsScope, name(), streamsMetrics);
        rangeSensor = StateStoreMetrics.rangeSensor(threadId, taskId, metricsScope, name(), streamsMetrics);
        flushSensor = StateStoreMetrics.flushSensor(threadId, taskId, metricsScope, name(), streamsMetrics);
        deleteSensor = StateStoreMetrics.deleteSensor(threadId, taskId, metricsScope, name(), streamsMetrics);
        final Sensor restoreSensor =
            StateStoreMetrics.restoreSensor(threadId, taskId, metricsScope, name(), streamsMetrics);

        // register and possibly restore the state from the logs
        maybeMeasureLatency(() -> super.init(context, root), time, restoreSensor);
    }

    @SuppressWarnings("unchecked")
    void initStoreSerde(final ProcessorContext context) {
        serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean setFlushListener(final CacheFlushListener<K, V> listener,
                                    final boolean sendOldValues) {
        final KeyValueStore<Bytes, byte[]> wrapped = wrapped();
        if (wrapped instanceof CachedStateStore) {
            return ((CachedStateStore<byte[], byte[]>) wrapped).setFlushListener(
                (rawKey, rawNewValue, rawOldValue, timestamp) -> listener.apply(
                    serdes.keyFrom(rawKey),
                    rawNewValue != null ? serdes.valueFrom(rawNewValue) : null,
                    rawOldValue != null ? serdes.valueFrom(rawOldValue) : null,
                    timestamp
                ),
                sendOldValues);
        }
        return false;
    }

    @Override
    public V get(final K key) {
        try {
            return maybeMeasureLatency(() -> outerValue(wrapped().get(keyBytes(key))), time, getSensor);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public void put(final K key,
                    final V value) {
        try {
            maybeMeasureLatency(() -> wrapped().put(keyBytes(key), serdes.rawValue(value)), time, putSensor);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, value);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public V putIfAbsent(final K key,
                         final V value) {
        return maybeMeasureLatency(
            () -> outerValue(wrapped().putIfAbsent(keyBytes(key), serdes.rawValue(value))),
            time,
            putIfAbsentSensor
        );
    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        maybeMeasureLatency(() -> wrapped().putAll(innerEntries(entries)), time, putAllSensor);
    }

    @Override
    public V delete(final K key) {
        try {
            return maybeMeasureLatency(() -> outerValue(wrapped().delete(keyBytes(key))), time, deleteSensor);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public KeyValueIterator<K, V> range(final K from,
                                        final K to) {
        return new MeteredKeyValueIterator(
            wrapped().range(Bytes.wrap(serdes.rawKey(from)), Bytes.wrap(serdes.rawKey(to))),
            rangeSensor
        );
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new MeteredKeyValueIterator(wrapped().all(), allSensor);
    }

    @Override
    public void flush() {
        maybeMeasureLatency(super::flush, time, flushSensor);
    }

    @Override
    public long approximateNumEntries() {
        return wrapped().approximateNumEntries();
    }

    @Override
    public void close() {
        try {
            wrapped().close();
        } finally {
            streamsMetrics.removeAllStoreLevelSensors(threadId, taskId, name());
        }
    }

    protected V outerValue(final byte[] value) {
        return value != null ? serdes.valueFrom(value) : null;
    }

    protected Bytes keyBytes(final K key) {
        return Bytes.wrap(serdes.rawKey(key));
    }

    private List<KeyValue<Bytes, byte[]>> innerEntries(final List<KeyValue<K, V>> from) {
        final List<KeyValue<Bytes, byte[]>> byteEntries = new ArrayList<>();
        for (final KeyValue<K, V> entry : from) {
            byteEntries.add(KeyValue.pair(Bytes.wrap(serdes.rawKey(entry.key)), serdes.rawValue(entry.value)));
        }
        return byteEntries;
    }

    private class MeteredKeyValueIterator implements KeyValueIterator<K, V> {

        private final KeyValueIterator<Bytes, byte[]> iter;
        private final Sensor sensor;
        private final long startNs;

        private MeteredKeyValueIterator(final KeyValueIterator<Bytes, byte[]> iter,
                                        final Sensor sensor) {
            this.iter = iter;
            this.sensor = sensor;
            this.startNs = time.nanoseconds();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            final KeyValue<Bytes, byte[]> keyValue = iter.next();
            return KeyValue.pair(
                serdes.keyFrom(keyValue.key.get()),
                outerValue(keyValue.value));
        }

        @Override
        public void close() {
            try {
                iter.close();
            } finally {
                sensor.record(time.nanoseconds() - startNs);
            }
        }

        @Override
        public K peekNextKey() {
            return serdes.keyFrom(iter.peekNextKey().get());
        }
    }
}
