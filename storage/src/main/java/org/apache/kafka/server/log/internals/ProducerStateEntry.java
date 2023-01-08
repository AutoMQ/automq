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
package org.apache.kafka.server.log.internals;

import org.apache.kafka.common.record.RecordBatch;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

/**
 * This class represents the state of a specific producer-id.
 * It contains batchMetadata queue which is ordered such that the batch with the lowest sequence is at the head of the
 * queue while the batch with the highest sequence is at the tail of the queue. We will retain at most {@link ProducerStateEntry#NUM_BATCHES_TO_RETAIN}
 * elements in the queue. When the queue is at capacity, we remove the first element to make space for the incoming batch.
 */
public class ProducerStateEntry {
    public static final int NUM_BATCHES_TO_RETAIN = 5;

    public int coordinatorEpoch;
    public long lastTimestamp;
    public OptionalLong currentTxnFirstOffset;

    private final long producerId;
    private final Deque<BatchMetadata> batchMetadata = new ArrayDeque<>();
    private short producerEpoch;

    public static ProducerStateEntry empty(long producerId) {
        return new ProducerStateEntry(producerId, RecordBatch.NO_PRODUCER_EPOCH, -1, RecordBatch.NO_TIMESTAMP, OptionalLong.empty(), Optional.empty());
    }

    public ProducerStateEntry(long producerId) {
        this(producerId, RecordBatch.NO_PRODUCER_EPOCH, -1, RecordBatch.NO_TIMESTAMP, OptionalLong.empty(), Optional.empty());
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
        maybeUpdateProducerEpoch(nextEntry.producerEpoch);
        while (!nextEntry.batchMetadata.isEmpty()) addBatchMetadata(nextEntry.batchMetadata.removeFirst());
        this.coordinatorEpoch = nextEntry.coordinatorEpoch;
        this.currentTxnFirstOffset = nextEntry.currentTxnFirstOffset;
        this.lastTimestamp = nextEntry.lastTimestamp;
    }

    public Optional<BatchMetadata> findDuplicateBatch(RecordBatch batch) {
        if (batch.producerEpoch() != producerEpoch) return Optional.empty();
        else return batchWithSequenceRange(batch.baseSequence(), batch.lastSequence());
    }

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