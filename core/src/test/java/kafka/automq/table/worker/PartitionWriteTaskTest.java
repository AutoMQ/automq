/*
 * Copyright 2026, AutoMQ HK Limited.
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

import kafka.cluster.Partition;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.PooledRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.FetchIsolation;

import com.automq.stream.utils.threads.EventLoop;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class PartitionWriteTaskTest {
    private static final int PARTITION_ID = 1;
    private static final long START_OFFSET = 100L;
    private static final long END_OFFSET = 102L;

    private final Writer writer = mock(Writer.class);
    private final Runnable releaseFetch = mock(Runnable.class);
    private CloseableIterator<Record> batchIterator;

    @AfterEach
    void closeBatchIterator() {
        if (batchIterator != null) {
            batchIterator.close();
        }
    }

    /**
     * A fetch that spans the end offset writes only in-range records and closes the active batch before release.
     */
    @Test
    void closesBatchWhenEndOffsetIsReached() throws Exception {
        PartitionWriteTaskContext context = runTask();

        ArgumentCaptor<Record> written = ArgumentCaptor.forClass(Record.class);
        verify(writer, times(2)).write(eq(PARTITION_ID), written.capture());
        assertEquals(List.of(100L, 101L), written.getAllValues().stream().map(Record::offset).toList());
        verify(writer).setEndOffset(PARTITION_ID, END_OFFSET);
        assertFalse(context.requireReset);
        verifyBatchClosedBeforeFetchRelease();
    }

    /**
     * A writer failure after decoding a record closes the active batch before release and requests a reset.
     */
    @Test
    void closesBatchWhenWriteFails() throws Exception {
        doThrow(new IOException("write failed")).when(writer).write(eq(PARTITION_ID), any());

        PartitionWriteTaskContext context = runTask();

        verify(writer).write(eq(PARTITION_ID), any());
        assertTrue(context.requireReset);
        verifyBatchClosedBeforeFetchRelease();
    }

    private PartitionWriteTaskContext runTask() throws Exception {
        Partition partition = TestUtils.partition(PARTITION_ID, 0);
        FetchDataInfo fetch = new FetchDataInfo(null, new PooledRecords(zstdRecords(), releaseFetch));
        when(partition.log().get().readAsync(eq(START_OFFSET), anyInt(), eq(FetchIsolation.TXN_COMMITTED), eq(true)))
            .thenReturn(CompletableFuture.completedFuture(fetch));
        when(writer.targetFileSize()).thenReturn(1024);

        EventLoop eventLoop = new EventLoop("partition-write-test");
        EventLoops.EventLoopRef eventLoopRef = new EventLoops.EventLoopRef(new EventLoops.EventLoopWrapper(eventLoop));
        try {
            PartitionWriteTaskContext context = new PartitionWriteTaskContext(writer, eventLoopRef, null, null, 0);
            PartitionWriteTask task = new PartitionWriteTask(partition, START_OFFSET, END_OFFSET, context);
            CompletableFuture.supplyAsync(task::run, eventLoop).thenCompose(completion -> completion).get(10, TimeUnit.SECONDS);
            return context;
        } finally {
            // The task future can complete before handleReadResult releases the fetch.
            eventLoop.shutdownGracefully().get(10, TimeUnit.SECONDS);
            eventLoopRef.release();
        }
    }

    private Records zstdRecords() {
        SimpleRecord record = new SimpleRecord(new byte[] {1});
        RecordBatch compressedBatch = MemoryRecords.withRecords(START_OFFSET, Compression.zstd().build(),
            record, record, record, record).batchIterator().next();
        RecordBatch batch = spy(compressedBatch);
        doAnswer(invocation -> {
            batchIterator = spy(compressedBatch.streamingIterator(invocation.getArgument(0)));
            return batchIterator;
        }).when(batch).streamingIterator(any());

        AbstractIterator<?> batches = mock(AbstractIterator.class);
        when(batches.hasNext()).thenReturn(true, false);
        doReturn(batch).when(batches).next();
        Records records = mock(Records.class);
        doReturn(batches).when(records).batchIterator();
        return records;
    }

    private void verifyBatchClosedBeforeFetchRelease() {
        InOrder cleanup = inOrder(batchIterator, releaseFetch);
        cleanup.verify(batchIterator).close();
        cleanup.verify(releaseFetch).run();
        verify(batchIterator).close();
    }
}
