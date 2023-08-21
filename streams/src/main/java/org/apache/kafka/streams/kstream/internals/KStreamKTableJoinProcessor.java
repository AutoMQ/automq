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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

class KStreamKTableJoinProcessor<K1, K2, V1, V2, VOut> extends ContextualProcessor<K1, V1, K1, VOut> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamKTableJoin.class);

    private final KTableValueGetter<K2, V2> valueGetter;
    private final KeyValueMapper<? super K1, ? super V1, ? extends K2> keyMapper;
    private final ValueJoinerWithKey<? super K1, ? super V1, ? super V2, ? extends VOut> joiner;
    private final boolean leftJoin;
    private Sensor droppedRecordsSensor;
    private final Optional<Duration> gracePeriod;
    private TimeOrderedKeyValueBuffer<K1, V1, V1> buffer;
    protected long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
    private InternalProcessorContext internalProcessorContext;
    private final boolean useBuffer;
    private final String storeName;

    KStreamKTableJoinProcessor(final KTableValueGetter<K2, V2> valueGetter,
                               final KeyValueMapper<? super K1, ? super V1, ? extends K2> keyMapper,
                               final ValueJoinerWithKey<? super K1, ? super V1, ? super V2, ? extends VOut> joiner,
                               final boolean leftJoin,
                               final Optional<Duration> gracePeriod,
                               final Optional<String> storeName) {
        this.valueGetter = valueGetter;
        this.keyMapper = keyMapper;
        this.joiner = joiner;
        this.leftJoin = leftJoin;
        this.useBuffer = gracePeriod.isPresent();
        this.gracePeriod = gracePeriod;
        this.storeName = storeName.orElse("");
    }

    @Override
    public void init(final ProcessorContext<K1, VOut> context) {
        super.init(context);
        final StreamsMetricsImpl metrics = (StreamsMetricsImpl) context.metrics();
        droppedRecordsSensor = droppedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
        valueGetter.init(context);
        internalProcessorContext = asInternalProcessorContext((org.apache.kafka.streams.processor.ProcessorContext) context);
        if (useBuffer) {
            if (!valueGetter.isVersioned() && gracePeriod.isPresent()) {
                throw new IllegalArgumentException("KTable must be versioned to use a grace period in a stream table join.");
            }
            buffer = requireNonNull(context.getStateStore(storeName));
            buffer.setSerdesIfNull(new SerdeGetter(context));
        }
    }

    @Override
    public void process(final Record<K1, V1> record) {
        internalProcessorContext = asInternalProcessorContext((org.apache.kafka.streams.processor.ProcessorContext) context());
        updateObservedStreamTime(record.timestamp());
        if (maybeDropRecord(record)) {
            return;
        }

        if (!useBuffer) {
            doJoin(record);
        } else {
            if (!buffer.put(observedStreamTime, record, internalProcessorContext.recordContext())) {
                doJoin(record);
            } else {
                buffer.evictWhile(() -> true, this::emit);
            }
        }
    }

    private void emit(final TimeOrderedKeyValueBuffer.Eviction<K1, V1> toEmit) {
        final Record<K1, V1> record = new Record<>(toEmit.key(), toEmit.value(), toEmit.recordContext().timestamp())
            .withHeaders(toEmit.recordContext().headers());
        final ProcessorRecordContext prevRecordContext = internalProcessorContext.recordContext();
        try {
            internalProcessorContext.setRecordContext(toEmit.recordContext());
            doJoin(record);
        } finally {
            internalProcessorContext.setRecordContext(prevRecordContext);
        }
    }

    protected void updateObservedStreamTime(final long timestamp) {
        observedStreamTime = Math.max(observedStreamTime, timestamp);
    }

    @SuppressWarnings("unchecked")
    private void doJoin(final Record<K1, V1> record) {
        final K2 mappedKey = keyMapper.apply(record.key(), record.value());
        final ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter.isVersioned()
            ? valueGetter.get(mappedKey, record.timestamp())
            : valueGetter.get(mappedKey);
        final V2 value2 = getValueOrNull(valueAndTimestamp2);
        if (leftJoin || value2 != null) {
            internalProcessorContext.forward(record.withValue(joiner.apply(record.key(), record.value(), value2)));
        }
    }

    private boolean maybeDropRecord(final Record<K1, V1> record) {
        // we do join iff the join keys are equal, thus, if {@code keyMapper} returns {@code null} we
        // cannot join and just ignore the record. Note for KTables, this is the same as having a null key
        // since keyMapper just returns the key, but for GlobalKTables we can have other keyMappers
        //
        // we also ignore the record if value is null, because in a key-value data model a null-value indicates
        // an empty message (ie, there is nothing to be joined) -- this contrast SQL NULL semantics
        // furthermore, on left/outer joins 'null' in ValueJoiner#apply() indicates a missing record --
        // thus, to be consistent and to avoid ambiguous null semantics, null values are ignored
        final K2 mappedKey = keyMapper.apply(record.key(), record.value());
        if (mappedKey == null || record.value() == null) {
            if (context().recordMetadata().isPresent()) {
                final RecordMetadata recordMetadata = context().recordMetadata().get();
                LOG.warn(
                    "Skipping record due to null join key or value. "
                        + "topic=[{}] partition=[{}] offset=[{}]",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                );
            } else {
                LOG.warn(
                    "Skipping record due to null join key or value. Topic, partition, and offset not known."
                );
            }
            droppedRecordsSensor.record();
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        valueGetter.close();
    }
}
