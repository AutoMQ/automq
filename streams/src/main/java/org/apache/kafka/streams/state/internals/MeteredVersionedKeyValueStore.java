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

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;

/**
 * A metered {@link VersionedKeyValueStore} wrapper that is used for recording operation
 * metrics, and hence its inner {@link VersionedBytesStore} implementation does not need to provide
 * its own metrics collecting functionality. The inner {@code VersionedBytesStore} of this class
 * is a {@link KeyValueStore} of type &lt;Bytes,byte[]&gt;, so we use {@link Serde}s
 * to convert from &lt;K,ValueAndTimestamp&lt;V&gt&gt; to &lt;Bytes,byte[]&gt;. In particular,
 * {@link NullableValueAndTimestampSerde} is used since putting a tombstone to a versioned key-value
 * store requires putting a null value associated with a timestamp.
 *
 * @param <K> The key type
 * @param <V> The (raw) value type
 */
public class MeteredVersionedKeyValueStore<K, V>
    extends WrappedStateStore<VersionedBytesStore, K, V>
    implements VersionedKeyValueStore<K, V> {

    private final MeteredVersionedKeyValueStoreInternal internal;

    MeteredVersionedKeyValueStore(final VersionedBytesStore inner,
                                  final String metricScope,
                                  final Time time,
                                  final Serde<K> keySerde,
                                  final Serde<ValueAndTimestamp<V>> valueSerde) {
        super(inner);
        internal = new MeteredVersionedKeyValueStoreInternal(inner, metricScope, time, keySerde, valueSerde);
    }

    /**
     * Conceptually, {@link MeteredVersionedKeyValueStore} should {@code extend}
     * {@link MeteredKeyValueStore}, but due to type conflicts, we cannot do this. (Specifically,
     * the first needs to be {@link VersionedKeyValueStore} while the second is {@link KeyValueStore}
     * and the two interfaces conflict.) Thus, we use an internal <it>instance</it> of
     * {@code MeteredKeyValueStore} to mimic inheritance instead.
     * <p>
     * It's not ideal because it requires an extra step to translate between the APIs of
     * {@link VersionedKeyValueStore} in {@link MeteredVersionedKeyValueStore} and
     * the APIs of {@link TimestampedKeyValueStore} in {@link MeteredVersionedKeyValueStoreInternal}.
     * This extra step is all that the methods of {@code MeteredVersionedKeyValueStoreInternal} do.
     * <p>
     * Note that the addition of {@link #get(Object, long)} and {@link #delete(Object, long)} in
     * this class are to match the interface of {@link VersionedKeyValueStore}.
     */
    private class MeteredVersionedKeyValueStoreInternal
        extends MeteredKeyValueStore<K, ValueAndTimestamp<V>> {

        private final VersionedBytesStore inner;

        MeteredVersionedKeyValueStoreInternal(final VersionedBytesStore inner,
                                              final String metricScope,
                                              final Time time,
                                              final Serde<K> keySerde,
                                              final Serde<ValueAndTimestamp<V>> valueSerde) {
            super(inner, metricScope, time, keySerde, valueSerde);
            this.inner = inner;
        }

        @Override
        public void put(final K key, final ValueAndTimestamp<V> value) {
            if (value == null) {
                throw new IllegalStateException("Versioned store requires timestamp associated with all puts, including tombstones/deletes");
            }
            super.put(key, value);
        }

        public ValueAndTimestamp<V> get(final K key, final long asOfTimestamp) {
            Objects.requireNonNull(key, "key cannot be null");
            try {
                return maybeMeasureLatency(() -> outerValue(inner.get(keyBytes(key), asOfTimestamp)), time, getSensor);
            } catch (final ProcessorStateException e) {
                final String message = String.format(e.getMessage(), key);
                throw new ProcessorStateException(message, e);
            }
        }

        public ValueAndTimestamp<V> delete(final K key, final long timestamp) {
            Objects.requireNonNull(key, "key cannot be null");
            try {
                return maybeMeasureLatency(() -> outerValue(inner.delete(keyBytes(key), timestamp)), time, deleteSensor);
            } catch (final ProcessorStateException e) {
                final String message = String.format(e.getMessage(), key);
                throw new ProcessorStateException(message, e);
            }
        }

        @Override
        protected <R> QueryResult<R> runRangeQuery(final Query<R> query,
                                                   final PositionBound positionBound,
                                                   final QueryConfig config) {
            // throw exception for now to reserve the ability to implement this in the future
            // without clashing with users' custom implementations in the meantime
            throw new UnsupportedOperationException("Versioned stores do not support RangeQuery queries at this time.");
        }

        @Override
        protected <R> QueryResult<R> runKeyQuery(final Query<R> query,
                                                 final PositionBound positionBound,
                                                 final QueryConfig config) {
            // throw exception for now to reserve the ability to implement this in the future
            // without clashing with users' custom implementations in the meantime
            throw new UnsupportedOperationException("Versioned stores do not support KeyQuery queries at this time.");
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Serde<ValueAndTimestamp<V>> prepareValueSerdeForStore(
            final Serde<ValueAndTimestamp<V>> valueSerde,
            final SerdeGetter getter
        ) {
            if (valueSerde == null) {
                return new NullableValueAndTimestampSerde<>((Serde<V>) getter.valueSerde());
            } else {
                return super.prepareValueSerdeForStore(valueSerde, getter);
            }
        }
    }

    @Override
    public void put(final K key, final V value, final long timestamp) {
        internal.put(key, ValueAndTimestamp.makeAllowNullable(value, timestamp));
    }

    @Override
    public VersionedRecord<V> delete(final K key, final long timestamp) {
        final ValueAndTimestamp<V> valueAndTimestamp = internal.delete(key, timestamp);
        return valueAndTimestamp == null
            ? null
            : new VersionedRecord<>(valueAndTimestamp.value(), valueAndTimestamp.timestamp());
    }

    @Override
    public VersionedRecord<V> get(final K key) {
        final ValueAndTimestamp<V> valueAndTimestamp = internal.get(key);
        return valueAndTimestamp == null
            ? null
            : new VersionedRecord<>(valueAndTimestamp.value(), valueAndTimestamp.timestamp());
    }

    @Override
    public VersionedRecord<V> get(final K key, final long asOfTimestamp) {
        final ValueAndTimestamp<V> valueAndTimestamp = internal.get(key, asOfTimestamp);
        return valueAndTimestamp == null
            ? null
            : new VersionedRecord<>(valueAndTimestamp.value(), valueAndTimestamp.timestamp());
    }

    @Override
    public String name() {
        return internal.name();
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        internal.init(context, root);
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        internal.init(context, root);
    }

    @Override
    public void flush() {
        internal.flush();
    }

    @Override
    public void close() {
        internal.close();
    }

    @Override
    public boolean persistent() {
        return internal.persistent();
    }

    @Override
    public boolean isOpen() {
        return internal.isOpen();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound, final QueryConfig config) {
        return internal.query(query, positionBound, config);
    }

    @Override
    public Position getPosition() {
        return internal.getPosition();
    }
}