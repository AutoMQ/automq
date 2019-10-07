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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.Objects;

public class TimestampedWindowStoreBuilder<K, V>
    extends AbstractStoreBuilder<K, ValueAndTimestamp<V>, TimestampedWindowStore<K, V>> {

    private final WindowBytesStoreSupplier storeSupplier;

    public TimestampedWindowStoreBuilder(final WindowBytesStoreSupplier storeSupplier,
                                         final Serde<K> keySerde,
                                         final Serde<V> valueSerde,
                                         final Time time) {
        super(storeSupplier.name(), keySerde, valueSerde == null ? null : new ValueAndTimestampSerde<>(valueSerde), time);
        Objects.requireNonNull(storeSupplier, "bytesStoreSupplier can't be null");
        this.storeSupplier = storeSupplier;
    }

    @Override
    public TimestampedWindowStore<K, V> build() {
        WindowStore<Bytes, byte[]> store = storeSupplier.get();
        if (!(store instanceof TimestampedBytesStore)) {
            if (store.persistent()) {
                store = new WindowToTimestampedWindowByteStoreAdapter(store);
            } else {
                store = new InMemoryTimestampedWindowStoreMarker(store);
            }
        }
        return new MeteredTimestampedWindowStore<>(
            maybeWrapCaching(maybeWrapLogging(store)),
            storeSupplier.windowSize(),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde);
    }

    private WindowStore<Bytes, byte[]> maybeWrapCaching(final WindowStore<Bytes, byte[]> inner) {
        if (!enableCaching) {
            return inner;
        }
        return new CachingWindowStore(
            inner,
            storeSupplier.windowSize(),
            storeSupplier.segmentIntervalMs());
    }

    private WindowStore<Bytes, byte[]> maybeWrapLogging(final WindowStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingTimestampedWindowBytesStore(inner, storeSupplier.retainDuplicates());
    }

    public long retentionPeriod() {
        return storeSupplier.retentionPeriod();
    }


    private final static class InMemoryTimestampedWindowStoreMarker
        implements WindowStore<Bytes, byte[]>, TimestampedBytesStore {

        private final WindowStore<Bytes, byte[]> wrapped;

        private InMemoryTimestampedWindowStoreMarker(final WindowStore<Bytes, byte[]> wrapped) {
            if (wrapped.persistent()) {
                throw new IllegalArgumentException("Provided store must not be a persistent store, but it is.");
            }
            this.wrapped = wrapped;
        }

        @Override
        public void init(final ProcessorContext context,
                         final StateStore root) {
            wrapped.init(context, root);
        }

        @Deprecated
        @Override
        public void put(final Bytes key,
                        final byte[] value) {
            wrapped.put(key, value);
        }

        @Override
        public void put(final Bytes key,
                        final byte[] value,
                        final long windowStartTimestamp) {
            wrapped.put(key, value, windowStartTimestamp);
        }

        @Override
        public byte[] fetch(final Bytes key,
                            final long time) {
            return wrapped.fetch(key, time);
        }

        @SuppressWarnings("deprecation")
        @Override
        public WindowStoreIterator<byte[]> fetch(final Bytes key,
                                                 final long timeFrom,
                                                 final long timeTo) {
            return wrapped.fetch(key, timeFrom, timeTo);
        }

        @SuppressWarnings("deprecation")
        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from,
                                                               final Bytes to,
                                                               final long timeFrom,
                                                               final long timeTo) {
            return wrapped.fetch(from, to, timeFrom, timeTo);
        }

        @SuppressWarnings("deprecation")
        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom,
                                                                  final long timeTo) {
            return wrapped.fetchAll(timeFrom, timeTo);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
            return wrapped.all();
        }

        @Override
        public void flush() {
            wrapped.flush();
        }

        @Override
        public void close() {
            wrapped.close();
        }
        @Override
        public boolean isOpen() {
            return wrapped.isOpen();
        }

        @Override
        public String name() {
            return wrapped.name();
        }

        @Override
        public boolean persistent() {
            return false;
        }
    }
}
