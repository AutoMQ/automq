/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package kafka.automq.table.worker;

import org.apache.kafka.clients.consumer.internals.CompletedFetch;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.storage.internals.log.FetchDataInfo;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * ref. {@link CompletedFetch}
 */
public class RecordsIterator extends AbstractIterator<Record> {
    private final Set<Long> abortedProducerIds = new HashSet<>();
    private final PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions;
    private final BufferSupplier bufferSupplier;

    private final Iterator<? extends RecordBatch> batches;
    private CloseableIterator<Record> records;
    private long nextOffset;

    public RecordsIterator(long startOffset, FetchDataInfo rst, BufferSupplier bufferSupplier) {
        this.nextOffset = startOffset;
        this.batches = rst.records.batchIterator();
        this.abortedTransactions = abortedTransactions(rst.abortedTransactions.orElse(null));
        this.bufferSupplier = bufferSupplier;
    }

    public long nextOffset() {
        return nextOffset;
    }

    @Override
    protected Record makeNext() {
        if (records != null && records.hasNext()) {
            Record record = records.next();
            nextOffset = record.offset() + 1;
            return record;
        }

        while (batches.hasNext()) {
            if (records != null) {
                records.close();
                records = null;
            }
            RecordBatch currentBatch = batches.next();
            if (currentBatch.hasProducerId()) {
                consumeAbortedTransactionsUpTo(currentBatch.lastOffset());
                long producerId = currentBatch.producerId();
                if (containsAbortMarker(currentBatch)) {
                    abortedProducerIds.remove(producerId);
                } else if (isBatchAborted(currentBatch)) {
                    nextOffset = currentBatch.nextOffset();
                    continue;
                }
            }
            if (currentBatch.isControlBatch()) {
                nextOffset = currentBatch.nextOffset();
                continue;
            }
            records = currentBatch.streamingIterator(bufferSupplier);
            return makeNext();
        }
        if (records != null) {
            records.close();
        }
        return allDone();
    }

    private void consumeAbortedTransactionsUpTo(long offset) {
        if (abortedTransactions == null)
            return;

        while (!abortedTransactions.isEmpty() && abortedTransactions.peek().firstOffset() <= offset) {
            FetchResponseData.AbortedTransaction abortedTransaction = abortedTransactions.poll();
            abortedProducerIds.add(abortedTransaction.producerId());
        }
    }

    private boolean isBatchAborted(RecordBatch batch) {
        return batch.isTransactional() && abortedProducerIds.contains(batch.producerId());
    }

    private PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions(
        List<FetchResponseData.AbortedTransaction> abortedTransactionList) {
        if (abortedTransactionList == null || abortedTransactionList.isEmpty())
            return null;

        PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions = new PriorityQueue<>(
            abortedTransactionList.size(), Comparator.comparingLong(FetchResponseData.AbortedTransaction::firstOffset)
        );
        abortedTransactions.addAll(abortedTransactionList);
        return abortedTransactions;
    }

    private boolean containsAbortMarker(RecordBatch batch) {
        if (!batch.isControlBatch())
            return false;

        Iterator<Record> batchIterator = batch.iterator();
        if (!batchIterator.hasNext())
            return false;

        Record firstRecord = batchIterator.next();
        return ControlRecordType.ABORT == ControlRecordType.parse(firstRecord.key());
    }
}
