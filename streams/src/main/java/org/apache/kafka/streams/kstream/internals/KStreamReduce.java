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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KStreamReduce<K, V> implements KStreamAggProcessorSupplier<K, V, K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(KStreamReduce.class);

    private final String storeName;
    private final Reducer<V> reducer;

    private boolean sendOldValues = false;

    KStreamReduce(final String storeName, final Reducer<V> reducer) {
        this.storeName = storeName;
        this.reducer = reducer;
    }

    @Override
    public Processor<K, V, K, Change<V>> get() {
        return new KStreamReduceProcessor();
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }


    private class KStreamReduceProcessor extends ContextualProcessor<K, V, K, Change<V>> {
        private TimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;
        private Sensor droppedRecordsSensor;

        @Override
        public void init(final ProcessorContext<K, Change<V>> context) {
            super.init(context);
            droppedRecordsSensor = droppedRecordsSensor(
                Thread.currentThread().getName(),
                context.taskId().toString(),
                (StreamsMetricsImpl) context.metrics()
            );
            store = context.getStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<>(
                store,
                context,
                new TimestampedCacheFlushListener<>(context),
                sendOldValues);
        }

        @Override
        public void process(final Record<K, V> record) {
            // If the key or value is null we don't need to proceed
            if (record.key() == null || record.value() == null) {
                if (context().recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context().recordMetadata().get();
                    LOG.warn(
                        "Skipping record due to null key or value. "
                            + "topic=[{}] partition=[{}] offset=[{}]",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                    );
                } else {
                    LOG.warn(
                        "Skipping record due to null key. Topic, partition, and offset not known."
                    );
                }
                droppedRecordsSensor.record();
                return;
            }

            final ValueAndTimestamp<V> oldAggAndTimestamp = store.get(record.key());
            final V oldAgg = getValueOrNull(oldAggAndTimestamp);

            final V newAgg;
            final long newTimestamp;

            if (oldAgg == null) {
                newAgg = record.value();
                newTimestamp = record.timestamp();
            } else {
                newAgg = reducer.apply(oldAgg, record.value());
                newTimestamp = Math.max(record.timestamp(), oldAggAndTimestamp.timestamp());
            }

            store.put(record.key(), ValueAndTimestamp.make(newAgg, newTimestamp));
            tupleForwarder.maybeForward(
                record.withValue(new Change<>(newAgg, sendOldValues ? oldAgg : null))
                    .withTimestamp(newTimestamp));
        }
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        return new KTableValueGetterSupplier<K, V>() {

            public KTableValueGetter<K, V> get() {
                return new KStreamReduceValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }


    private class KStreamReduceValueGetter implements KTableValueGetter<K, V> {
        private TimestampedKeyValueStore<K, V> store;

        @Override
        public void init(final ProcessorContext<?, ?> context) {
            store = context.getStateStore(storeName);
        }

        @Override
        public ValueAndTimestamp<V> get(final K key) {
            return store.get(key);
        }
    }
}

