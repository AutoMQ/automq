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
package org.apache.kafka.streams.kstream;


/**
 * The {@code Aggregator} interface for aggregating values of the given key.
 * This is a generalization of {@link Reducer} and allows to have different types for input value and aggregation
 * result.
 * {@code Aggregator} is used in combination with {@link Initializer} that provides an initial aggregation value.
 * <p>
 * {@code Aggregator} can be used to implement aggregation functions like count.
 *
 * @param <K> key type
 * @param <V> input value type
 * @param <VAgg> aggregate value type
 *
 * @see Initializer
 * @see KGroupedStream#aggregate(Initializer, Aggregator)
 * @see KGroupedStream#aggregate(Initializer, Aggregator, Materialized)
 * @see TimeWindowedKStream#aggregate(Initializer, Aggregator)
 * @see TimeWindowedKStream#aggregate(Initializer, Aggregator, Materialized)
 * @see SessionWindowedKStream#aggregate(Initializer, Aggregator, Merger)
 * @see SessionWindowedKStream#aggregate(Initializer, Aggregator, Merger, Materialized)
 * @see Reducer
 */
public interface Aggregator<K, V, VAgg> {

    /**
     * Compute a new aggregate from the key and value of a record and the current aggregate of the same key.
     *
     * @param key
     *        the key of the record
     * @param value
     *        the value of the record
     * @param aggregate
     *        the current aggregate value
     *
     * @return the new aggregate value
     */
    VAgg apply(final K key, final V value, final VAgg aggregate);
}
