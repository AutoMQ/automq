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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;

import java.util.ArrayDeque;


/**
 * RecordQueue is a FIFO queue of {@link StampedRecord} (ConsumerRecord + timestamp). It also keeps track of the
 * partition timestamp defined as the minimum timestamp of records in its queue; in addition, its partition
 * timestamp is monotonically increasing such that once it is advanced, it will not be decremented.
 */
public class RecordQueue {

    static final long UNKNOWN = -1L;

    private final Logger log;
    private final SourceNode source;
    private final TopicPartition partition;
    private final ProcessorContext processorContext;
    private final TimestampExtractor timestampExtractor;
    private final RecordDeserializer recordDeserializer;
    private final ArrayDeque<ConsumerRecord<byte[], byte[]>> fifoQueue;

    private long partitionTime = UNKNOWN;
    private StampedRecord headRecord = null;

    RecordQueue(final TopicPartition partition,
                final SourceNode source,
                final TimestampExtractor timestampExtractor,
                final DeserializationExceptionHandler deserializationExceptionHandler,
                final InternalProcessorContext processorContext,
                final LogContext logContext) {
        this.source = source;
        this.partition = partition;
        this.fifoQueue = new ArrayDeque<>();
        this.timestampExtractor = timestampExtractor;
        this.recordDeserializer = new RecordDeserializer(
            source,
            deserializationExceptionHandler,
            logContext,
            processorContext.metrics().skippedRecordsSensor()
        );
        this.processorContext = processorContext;
        this.log = logContext.logger(RecordQueue.class);
    }

    /**
     * Returns the corresponding source node in the topology
     *
     * @return SourceNode
     */
    public SourceNode source() {
        return source;
    }

    /**
     * Returns the partition with which this queue is associated
     *
     * @return TopicPartition
     */
    public TopicPartition partition() {
        return partition;
    }

    /**
     * Add a batch of {@link ConsumerRecord} into the queue
     *
     * @param rawRecords the raw records
     * @return the size of this queue
     */
    int addRawRecords(final Iterable<ConsumerRecord<byte[], byte[]>> rawRecords) {
        for (final ConsumerRecord<byte[], byte[]> rawRecord : rawRecords) {
            fifoQueue.addLast(rawRecord);
        }

        maybeUpdateTimestamp();

        return size();
    }

    /**
     * Get the next {@link StampedRecord} from the queue
     *
     * @return StampedRecord
     */
    public StampedRecord poll() {
        final StampedRecord recordToReturn = headRecord;
        headRecord = null;

        maybeUpdateTimestamp();

        return recordToReturn;
    }

    /**
     * Returns the number of records in the queue
     *
     * @return the number of records
     */
    public int size() {
        // plus one deserialized head record for timestamp tracking
        return fifoQueue.size() + (headRecord == null ? 0 : 1);
    }

    /**
     * Tests if the queue is empty
     *
     * @return true if the queue is empty, otherwise false
     */
    public boolean isEmpty() {
        return fifoQueue.isEmpty() && headRecord == null;
    }

    /**
     * Returns the tracked partition timestamp
     *
     * @return timestamp
     */
    public long timestamp() {
        return partitionTime;
    }

    /**
     * Clear the fifo queue of its elements, also clear the time tracker's kept stamped elements
     */
    public void clear() {
        fifoQueue.clear();
        headRecord = null;
        partitionTime = UNKNOWN;
    }

    private void maybeUpdateTimestamp() {
        while (headRecord == null && !fifoQueue.isEmpty()) {
            final ConsumerRecord<byte[], byte[]> raw = fifoQueue.pollFirst();
            final ConsumerRecord<Object, Object> deserialized = recordDeserializer.deserialize(processorContext, raw);

            if (deserialized == null) {
                // this only happens if the deserializer decides to skip. It has already logged the reason.
                continue;
            }

            final long timestamp;
            try {
                timestamp = timestampExtractor.extract(deserialized, partitionTime);
            } catch (final StreamsException internalFatalExtractorException) {
                throw internalFatalExtractorException;
            } catch (final Exception fatalUserException) {
                throw new StreamsException(
                        String.format("Fatal user code error in TimestampExtractor callback for record %s.", deserialized),
                        fatalUserException);
            }
            log.trace("Source node {} extracted timestamp {} for record {}", source.name(), timestamp, deserialized);

            // drop message if TS is invalid, i.e., negative
            if (timestamp < 0) {
                log.warn(
                        "Skipping record due to negative extracted timestamp. topic=[{}] partition=[{}] offset=[{}] extractedTimestamp=[{}] extractor=[{}]",
                        deserialized.topic(), deserialized.partition(), deserialized.offset(), timestamp, timestampExtractor.getClass().getCanonicalName()
                );
                ((StreamsMetricsImpl) processorContext.metrics()).skippedRecordsSensor().record();
                continue;
            }

            headRecord = new StampedRecord(deserialized, timestamp);

            // update the partition timestamp if the current head record's timestamp has exceed its value
            if (timestamp > partitionTime) {
                partitionTime = timestamp;
            }
        }
    }
}
