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
package org.apache.kafka.streams.state;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;

import java.time.Instant;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

/**
 * Interface for storing the aggregated values of fixed-size time windows.
 * <p>
 * Note, that the stores's physical key type is {@link Windowed Windowed&lt;K&gt;}.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
public interface WindowStore<K, V> extends StateStore, ReadOnlyWindowStore<K, V> {

    /**
     * Use the current record timestamp as the {@code windowStartTimestamp} and
     * delegate to {@link WindowStore#put(Object, Object, long)}.
     *
     * It's highly recommended to use {@link WindowStore#put(Object, Object, long)} instead, as the record timestamp
     * is unlikely to be the correct windowStartTimestamp in general.
     *
     * @param key The key to associate the value to
     * @param value The value to update, it can be null;
     *              if the serialized bytes are also null it is interpreted as deletes
     * @throws NullPointerException if the given key is {@code null}
     */
    void put(K key, V value);

    /**
     * Put a key-value pair into the window with given window start timestamp
     * @param key The key to associate the value to
     * @param value The value; can be null
     * @param windowStartTimestamp The timestamp of the beginning of the window to put the key/value into
     * @throws NullPointerException if the given key is {@code null}
     */
    void put(K key, V value, long windowStartTimestamp);

    /**
     * Get all the key-value pairs with the given key and the time range from all the existing windows.
     * <p>
     * This iterator must be closed after use.
     * <p>
     * The time range is inclusive and applies to the starting timestamp of the window.
     * For example, if we have the following windows:
     * <pre>
     * +-------------------------------+
     * |  key  | start time | end time |
     * +-------+------------+----------+
     * |   A   |     10     |    20    |
     * +-------+------------+----------+
     * |   A   |     15     |    25    |
     * +-------+------------+----------+
     * |   A   |     20     |    30    |
     * +-------+------------+----------+
     * |   A   |     25     |    35    |
     * +--------------------------------
     * </pre>
     * And we call {@code store.fetch("A", 10, 20)} then the results will contain the first
     * three windows from the table above, i.e., all those where 10 &lt;= start time &lt;= 20.
     * <p>
     * For each key, the iterator guarantees ordering of windows, starting from the oldest/earliest
     * available window to the newest/latest window.
     *
     * @param key       the key to fetch
     * @param timeFrom  time range start (inclusive)
     * @param timeTo    time range end (inclusive)
     * @return an iterator over key-value pairs {@code <timestamp, value>}
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException if the given key is {@code null}
     */
    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetch(...) is removed
    WindowStoreIterator<V> fetch(K key, long timeFrom, long timeTo);

    @Override
    default WindowStoreIterator<V> fetch(final K key,
                                         final Instant from,
                                         final Instant to) {
        return fetch(
            key,
            ApiUtils.validateMillisecondInstant(from, prepareMillisCheckFailMsgPrefix(from, "from")),
            ApiUtils.validateMillisecondInstant(to, prepareMillisCheckFailMsgPrefix(to, "to")));
    }

    /**
     * Get all the key-value pairs in the given key range and time range from all the existing windows.
     * <p>
     * This iterator must be closed after use.
     *
     * @param from      the first key in the range
     * @param to        the last key in the range
     * @param timeFrom  time range start (inclusive)
     * @param timeTo    time range end (inclusive)
     * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException if one of the given keys is {@code null}
     */
    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetch(...) is removed
    KeyValueIterator<Windowed<K>, V> fetch(K from, K to, long timeFrom, long timeTo);

    @Override
    default KeyValueIterator<Windowed<K>, V> fetch(final K from,
                                                   final K to,
                                                   final Instant fromTime,
                                                   final Instant toTime) {
        return fetch(
            from,
            to,
            ApiUtils.validateMillisecondInstant(fromTime, prepareMillisCheckFailMsgPrefix(fromTime, "fromTime")),
            ApiUtils.validateMillisecondInstant(toTime, prepareMillisCheckFailMsgPrefix(toTime, "toTime")));
    }

    /**
     * Gets all the key-value pairs that belong to the windows within in the given time range.
     *
     * @param timeFrom the beginning of the time slot from which to search (inclusive)
     * @param timeTo   the end of the time slot from which to search (inclusive)
     * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}
     * @throws InvalidStateStoreException if the store is not initialized
     */
    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetchAll(...) is removed
    KeyValueIterator<Windowed<K>, V> fetchAll(long timeFrom, long timeTo);

    @Override
    default KeyValueIterator<Windowed<K>, V> fetchAll(final Instant from, final Instant to) {
        return fetchAll(
            ApiUtils.validateMillisecondInstant(from, prepareMillisCheckFailMsgPrefix(from, "from")),
            ApiUtils.validateMillisecondInstant(to, prepareMillisCheckFailMsgPrefix(to, "to")));
    }
}
