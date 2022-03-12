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

import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;

class WindowStoreIteratorWrapper {

    private final KeyValueIterator<Bytes, byte[]> bytesIterator;
    private final long windowSize;
    private final Function<byte[], Long> timestampExtractor;
    private final BiFunction<byte[], Long, Windowed<Bytes>> windowConstructor;

    WindowStoreIteratorWrapper(final KeyValueIterator<Bytes, byte[]> bytesIterator,
                               final long windowSize) {
        this(bytesIterator, windowSize, WindowKeySchema::extractStoreTimestamp, WindowKeySchema::fromStoreBytesKey);
    }

    WindowStoreIteratorWrapper(final KeyValueIterator<Bytes, byte[]> bytesIterator,
                               final long windowSize,
                               final Function<byte[], Long> timestampExtractor,
                               final BiFunction<byte[], Long, Windowed<Bytes>> windowConstructor) {
        this.bytesIterator = bytesIterator;
        this.windowSize = windowSize;
        this.timestampExtractor = timestampExtractor;
        this.windowConstructor = windowConstructor;
    }

    public WindowStoreIterator<byte[]> valuesIterator() {
        return new WrappedWindowStoreIterator(bytesIterator, timestampExtractor);
    }

    public KeyValueIterator<Windowed<Bytes>, byte[]> keyValueIterator() {
        return new WrappedKeyValueIterator(bytesIterator, windowSize, windowConstructor);
    }

    private static class WrappedWindowStoreIterator implements WindowStoreIterator<byte[]> {
        final KeyValueIterator<Bytes, byte[]> bytesIterator;
        final Function<byte[], Long> timestampExtractor;

        WrappedWindowStoreIterator(
            final KeyValueIterator<Bytes, byte[]> bytesIterator,
            final Function<byte[], Long> timestampExtractor) {
            this.bytesIterator = bytesIterator;
            this.timestampExtractor = timestampExtractor;
        }

        @Override
        public Long peekNextKey() {
            return timestampExtractor.apply(bytesIterator.peekNextKey().get());
        }

        @Override
        public boolean hasNext() {
            return bytesIterator.hasNext();
        }

        @Override
        public KeyValue<Long, byte[]> next() {
            final KeyValue<Bytes, byte[]> next = bytesIterator.next();
            final long timestamp = timestampExtractor.apply(next.key.get());
            return KeyValue.pair(timestamp, next.value);
        }

        @Override
        public void close() {
            bytesIterator.close();
        }
    }

    private static class WrappedKeyValueIterator implements KeyValueIterator<Windowed<Bytes>, byte[]> {
        final KeyValueIterator<Bytes, byte[]> bytesIterator;
        final long windowSize;
        final BiFunction<byte[], Long, Windowed<Bytes>> windowConstructor;

        WrappedKeyValueIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator,
                                final long windowSize,
                                final BiFunction<byte[], Long, Windowed<Bytes>> windowConstructor) {
            this.bytesIterator = bytesIterator;
            this.windowSize = windowSize;
            this.windowConstructor = windowConstructor;
        }

        @Override
        public Windowed<Bytes> peekNextKey() {
            final byte[] nextKey = bytesIterator.peekNextKey().get();
            return windowConstructor.apply(nextKey, windowSize);
        }

        @Override
        public boolean hasNext() {
            return bytesIterator.hasNext();
        }

        @Override
        public KeyValue<Windowed<Bytes>, byte[]> next() {
            final KeyValue<Bytes, byte[]> next = bytesIterator.next();
            return KeyValue.pair(windowConstructor.apply(next.key.get(), windowSize), next.value);
        }

        @Override
        public void close() {
            bytesIterator.close();
        }
    }
}
