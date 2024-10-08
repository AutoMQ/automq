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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.errors.DuplicateSequenceException;
import org.apache.kafka.common.record.RecordBatch;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static org.apache.kafka.common.record.DefaultRecordBatch.decrementSequence;

/**
 * This class represents the state of a specific producer-id.
 * It contains batchMetadata queue which is ordered such that the batch with the lowest sequence is at the head of the
 * queue while the batch with the highest sequence is at the tail of the queue. We will retain at most {@link ProducerStateEntry#NUM_BATCHES_TO_RETAIN}
 * elements in the queue. When the queue is at capacity, we remove the first element to make space for the incoming batch.
 */
public class ProducerStateEntry {
    public static final int NUM_BATCHES_TO_RETAIN = 5;
    private final long producerId;
    private final Deque<BatchMetadata> batchMetadata = new ArrayDeque<>();

    private short producerEpoch;
    private int coordinatorEpoch;
    private long lastTimestamp;
    private OptionalLong currentTxnFirstOffset;

    public static ProducerStateEntry empty(long producerId) {
        return new ProducerStateEntry(producerId, RecordBatch.NO_PRODUCER_EPOCH, -1, RecordBatch.NO_TIMESTAMP, OptionalLong.empty(), Optional.empty());
    }

    public ProducerStateEntry(long producerId, short producerEpoch, int coordinatorEpoch, long lastTimestamp, OptionalLong currentTxnFirstOffset, Optional<BatchMetadata> firstBatchMetadata) {
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.coordinatorEpoch = coordinatorEpoch;
        this.lastTimestamp = lastTimestamp;
        this.currentTxnFirstOffset = currentTxnFirstOffset;
        firstBatchMetadata.ifPresent(batchMetadata::add);
    }

    public int firstSeq() {
        return isEmpty() ? RecordBatch.NO_SEQUENCE : batchMetadata.getFirst().firstSeq();
    }

    public int lastSeq() {
        return isEmpty() ? RecordBatch.NO_SEQUENCE : batchMetadata.getLast().lastSeq;
    }

    public long firstDataOffset() {
        return isEmpty() ? -1L : batchMetadata.getFirst().firstOffset();
    }

    public long lastDataOffset() {
        return isEmpty() ? -1L : batchMetadata.getLast().lastOffset;
    }

    public int lastOffsetDelta() {
        return isEmpty() ? 0 : batchMetadata.getLast().offsetDelta;
    }

    public boolean isEmpty() {
        return batchMetadata.isEmpty();
    }

    /**
     * Returns a new instance with the provided parameters (when present) and the values from the current instance
     * otherwise.
     */
    public ProducerStateEntry withProducerIdAndBatchMetadata(long producerId, Optional<BatchMetadata> batchMetadata) {
        return new ProducerStateEntry(producerId, this.producerEpoch(), this.coordinatorEpoch, this.lastTimestamp,
            this.currentTxnFirstOffset, batchMetadata);
    }

    public void addBatch(short producerEpoch, int lastSeq, long lastOffset, int offsetDelta, long timestamp) {
        maybeUpdateProducerEpoch(producerEpoch);
        addBatchMetadata(new BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp));
        this.lastTimestamp = timestamp;
    }

    public boolean maybeUpdateProducerEpoch(short producerEpoch) {
        if (this.producerEpoch != producerEpoch) {
            batchMetadata.clear();
            this.producerEpoch = producerEpoch;
            return true;
        } else {
            return false;
        }
    }

    private void addBatchMetadata(BatchMetadata batch) {
        if (batchMetadata.size() == ProducerStateEntry.NUM_BATCHES_TO_RETAIN) batchMetadata.removeFirst();
        batchMetadata.add(batch);
    }

    public void update(ProducerStateEntry nextEntry) {
        update(nextEntry.producerEpoch, nextEntry.coordinatorEpoch, nextEntry.lastTimestamp, nextEntry.batchMetadata, nextEntry.currentTxnFirstOffset);
    }

    public void update(short producerEpoch, int coordinatorEpoch, long lastTimestamp) {
        update(producerEpoch, coordinatorEpoch, lastTimestamp, new ArrayDeque<>(0), OptionalLong.empty());
    }

    private void update(short producerEpoch, int coordinatorEpoch, long lastTimestamp, Deque<BatchMetadata> batchMetadata,
                        OptionalLong currentTxnFirstOffset) {
        maybeUpdateProducerEpoch(producerEpoch);
        while (!batchMetadata.isEmpty())
            addBatchMetadata(batchMetadata.removeFirst());
        this.coordinatorEpoch = coordinatorEpoch;
        this.currentTxnFirstOffset = currentTxnFirstOffset;
        this.lastTimestamp = lastTimestamp;
    }

    public void setCurrentTxnFirstOffset(long firstOffset) {
        this.currentTxnFirstOffset = OptionalLong.of(firstOffset);
    }

    public Optional<BatchMetadata> findDuplicateBatch(RecordBatch batch) {
        // AutoMQ inject start
        if (batch.producerEpoch() != producerEpoch) {
            return Optional.empty();
        }
        Optional<BatchMetadata> metadata = batchWithSequenceRange(batch.baseSequence(), batch.lastSequence());
        if (metadata.isPresent()) {
            return metadata;
        }
        BatchMetadata front = batchMetadata.peek();
        if (front != null && front.recovered) {
            // the batch metadata (`front`) is recovered from snapshot
            boolean batchFallInFront = contains(front.firstSeq(), front.lastSeq, batch.baseSequence())
                && contains(front.firstSeq(), front.lastSeq, batch.lastSequence());
            if (batchFallInFront) {
                throw new DuplicateSequenceException(
                    String.format("The batch is duplicated (recover from snapshot), broker cached metadata is %s, the record batch is [%s, %s]",
                        this, batch.baseSequence(), batch.lastSequence())
                );
            }
        }
        if (front != null) {
            // regard the batch as duplicated if it is before the first cached batch
            boolean batchBeforeFront = contains(
                decrementSequence(front.firstSeq(), 65536),
                decrementSequence(front.firstSeq(), 1),
                batch.lastSequence()
            );
            if (batchBeforeFront) {
                throw new DuplicateSequenceException(
                    String.format("The batch is duplicated, broker cached metadata is %s, the record batch is [%s, %s]",
                        this, batch.baseSequence(), batch.lastSequence())
                );
            }
        }
        return metadata;
        // AutoMQ inject end
    }

    // AutoMQ inject start
    /**
     * Check if the sequence number is in the range [start, end] (inclusive).
     * The range may wrap around the sequence space.
     */
    private static boolean contains(int start, int end, int seq) {
        if (start <= end) {
            return seq >= start && seq <= end;
        } else {
            // wrap around
            return seq >= start || seq <= end;
        }
    }
    // AutoMQ inject end

    // Return the batch metadata of the cached batch having the exact sequence range, if any.
    Optional<BatchMetadata> batchWithSequenceRange(int firstSeq, int lastSeq) {
        Stream<BatchMetadata> duplicate = batchMetadata.stream().filter(metadata -> firstSeq == metadata.firstSeq() && lastSeq == metadata.lastSeq);
        return duplicate.findFirst();
    }

    public Collection<BatchMetadata> batchMetadata() {
        return Collections.unmodifiableCollection(batchMetadata);
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public long producerId() {
        return producerId;
    }

    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    public long lastTimestamp() {
        return lastTimestamp;
    }

    public OptionalLong currentTxnFirstOffset() {
        return currentTxnFirstOffset;
    }

    @Override
    public String toString() {
        return "ProducerStateEntry(" +
                "producerId=" + producerId +
                ", producerEpoch=" + producerEpoch +
                ", currentTxnFirstOffset=" + currentTxnFirstOffset +
                ", coordinatorEpoch=" + coordinatorEpoch +
                ", lastTimestamp=" + lastTimestamp +
                ", batchMetadata=" + batchMetadata +
                ')';
    }
}
