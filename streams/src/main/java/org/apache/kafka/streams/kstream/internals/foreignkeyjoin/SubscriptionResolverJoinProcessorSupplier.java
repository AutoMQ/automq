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

package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.Murmur3;

/**
 * Receives {@code SubscriptionResponseWrapper<VO>} events and filters out events which do not match the current hash
 * of the primary key. This eliminates race-condition results for rapidly-changing foreign-keys for a given primary key.
 * Applies the join and emits nulls according to LEFT/INNER rules.
 *
 * @param <K> Type of primary keys
 * @param <V> Type of primary values
 * @param <VO> Type of foreign values
 * @param <VR> Type of joined result of primary and foreign values
 */
public class SubscriptionResolverJoinProcessorSupplier<K, V, VO, VR> implements ProcessorSupplier<K, SubscriptionResponseWrapper<VO>> {
    private final KTableValueGetterSupplier<K, V> valueGetterSupplier;
    private final Serializer<V> constructionTimeValueSerializer;
    private final ValueJoiner<V, VO, VR> joiner;
    private final boolean leftJoin;

    public SubscriptionResolverJoinProcessorSupplier(final KTableValueGetterSupplier<K, V> valueGetterSupplier,
                                                     final Serializer<V> valueSerializer,
                                                     final ValueJoiner<V, VO, VR> joiner,
                                                     final boolean leftJoin) {
        this.valueGetterSupplier = valueGetterSupplier;
        constructionTimeValueSerializer = valueSerializer;
        this.joiner = joiner;
        this.leftJoin = leftJoin;
    }

    @Override
    public Processor<K, SubscriptionResponseWrapper<VO>> get() {
        return new AbstractProcessor<K, SubscriptionResponseWrapper<VO>>() {
            private Serializer<V> runtimeValueSerializer = constructionTimeValueSerializer;

            private KTableValueGetter<K, V> valueGetter;

            @SuppressWarnings("unchecked")
            @Override
            public void init(final ProcessorContext context) {
                super.init(context);
                valueGetter = valueGetterSupplier.get();
                valueGetter.init(context);
                if (runtimeValueSerializer == null) {
                    runtimeValueSerializer = (Serializer<V>) context.valueSerde().serializer();
                }
            }

            @Override
            public void process(final K key, final SubscriptionResponseWrapper<VO> value) {
                if (value.getVersion() != SubscriptionResponseWrapper.CURRENT_VERSION) {
                    //Guard against modifications to SubscriptionResponseWrapper. Need to ensure that there is
                    //compatibility with previous versions to enable rolling upgrades. Must develop a strategy for
                    //upgrading from older SubscriptionWrapper versions to newer versions.
                    throw new UnsupportedVersionException("SubscriptionResponseWrapper is of an incompatible version.");
                }
                final ValueAndTimestamp<V> currentValueWithTimestamp = valueGetter.get(key);

                //We are unable to access the actual source topic name for the valueSerializer at runtime, without
                //tightly coupling to KTableRepartitionProcessorSupplier.
                //While we can use the source topic from where the events came from, we shouldn't serialize against it
                //as it causes problems with the confluent schema registry, which requires each topic have only a single
                //registered schema.
                final String dummySerializationTopic = context().topic() + "-join-resolver";
                final long[] currentHash = currentValueWithTimestamp == null ?
                    null :
                    Murmur3.hash128(runtimeValueSerializer.serialize(dummySerializationTopic, currentValueWithTimestamp.value()));

                final long[] messageHash = value.getOriginalValueHash();

                //If this value doesn't match the current value from the original table, it is stale and should be discarded.
                if (java.util.Arrays.equals(messageHash, currentHash)) {
                    final VR result;

                    if (value.getForeignValue() == null && (!leftJoin || currentValueWithTimestamp == null)) {
                        result = null; //Emit tombstone
                    } else {
                        result = joiner.apply(currentValueWithTimestamp == null ? null : currentValueWithTimestamp.value(), value.getForeignValue());
                    }
                    context().forward(key, result);
                }
            }
        };
    }
}
