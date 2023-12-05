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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.cache.DefaultS3BlockCache.ReadAheadTaskKey;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.utils.CloseableIterator;
import com.automq.stream.utils.Threads;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamReaderTest {

    @Test
    public void testSyncReadAheadInflight() {
        DataBlockReadAccumulator accumulator = new DataBlockReadAccumulator();
        ObjectReaderLRUCache cache = Mockito.mock(ObjectReaderLRUCache.class);
        S3Operator s3Operator = Mockito.mock(S3Operator.class);
        ObjectManager objectManager = Mockito.mock(ObjectManager.class);
        BlockCache blockCache = Mockito.mock(BlockCache.class);
        Map<ReadAheadTaskKey, CompletableFuture<Void>> inflightReadAheadTasks = new HashMap<>();
        StreamReader streamReader = new StreamReader(s3Operator, objectManager, blockCache, cache, accumulator, inflightReadAheadTasks, new InflightReadThrottle());

        long startOffset = 70;
        StreamReader.ReadContext context = new StreamReader.ReadContext(startOffset, 256);
        ObjectReader.DataBlockIndex index1 = new ObjectReader.DataBlockIndex(0, 0, 256, 128);
        context.streamDataBlocksPair = List.of(
                new ImmutablePair<>(1L, List.of(
                        new StreamDataBlock(233L, 64, 128, 1, index1))));

        ObjectReader reader = Mockito.mock(ObjectReader.class);
        Mockito.when(reader.read(index1)).thenReturn(new CompletableFuture<>());
        context.objectReaderMap = new HashMap<>(Map.of(1L, reader));
        inflightReadAheadTasks.put(new ReadAheadTaskKey(233L, startOffset), new CompletableFuture<>());
        streamReader.handleSyncReadAhead(233L, startOffset,
                999, 64, Mockito.mock(ReadAheadAgent.class), UUID.randomUUID(), new TimerUtil(), context);
        Threads.sleep(1000);
        Assertions.assertEquals(2, inflightReadAheadTasks.size());
        Assertions.assertTrue(inflightReadAheadTasks.containsKey(new ReadAheadTaskKey(233L, startOffset)));
        Assertions.assertTrue(inflightReadAheadTasks.containsKey(new ReadAheadTaskKey(233L, 64)));
    }

    @Test
    public void testSyncReadAhead() {
        DataBlockReadAccumulator accumulator = new DataBlockReadAccumulator();
        ObjectReaderLRUCache cache = Mockito.mock(ObjectReaderLRUCache.class);
        S3Operator s3Operator = Mockito.mock(S3Operator.class);
        ObjectManager objectManager = Mockito.mock(ObjectManager.class);
        BlockCache blockCache = Mockito.mock(BlockCache.class);
        StreamReader streamReader = new StreamReader(s3Operator, objectManager, blockCache, cache, accumulator, new HashMap<>(), new InflightReadThrottle());

        StreamReader.ReadContext context = new StreamReader.ReadContext(0, 256);
        ObjectReader.DataBlockIndex index1 = new ObjectReader.DataBlockIndex(0, 0, 256, 128);
        context.streamDataBlocksPair = List.of(
                new ImmutablePair<>(1L, List.of(
                        new StreamDataBlock(233L, 0, 128, 1, index1))));

        ObjectReader reader = Mockito.mock(ObjectReader.class);
        ObjectReader.DataBlock dataBlock1 = Mockito.mock(ObjectReader.DataBlock.class);
        StreamRecordBatch record1 = new StreamRecordBatch(233L, 0, 0, 64, TestUtils.random(128));
        record1.release();
        StreamRecordBatch record2 = new StreamRecordBatch(233L, 0, 64, 64, TestUtils.random(128));
        record2.release();
        List<StreamRecordBatch> records = List.of(record1, record2);
        AtomicInteger remaining = new AtomicInteger(0);
        Assertions.assertEquals(1, record1.getPayload().refCnt());
        Assertions.assertEquals(1, record2.getPayload().refCnt());
        Mockito.when(dataBlock1.iterator()).thenReturn(new CloseableIterator<>() {
            @Override
            public void close() {
            }

            @Override
            public boolean hasNext() {
                return remaining.get() < records.size();
            }

            @Override
            public StreamRecordBatch next() {
                if (!hasNext()) {
                    throw new IllegalStateException("no more elements");
                }
                return records.get(remaining.getAndIncrement());
            }
        });
        Mockito.when(reader.read(index1)).thenReturn(CompletableFuture.completedFuture(dataBlock1));
        context.objectReaderMap = new HashMap<>(Map.of(1L, reader));
        CompletableFuture<List<StreamRecordBatch>> cf = streamReader.handleSyncReadAhead(233L, 0,
                999, 64, Mockito.mock(ReadAheadAgent.class), UUID.randomUUID(), new TimerUtil(), context);

        cf.whenComplete((rst, ex) -> {
            Assertions.assertNull(ex);
            Assertions.assertEquals(1, rst.size());
            Assertions.assertTrue(record1.equals(rst.get(0)));
            Assertions.assertEquals(2, record1.getPayload().refCnt());
            Assertions.assertEquals(1, record2.getPayload().refCnt());
        }).join();
    }

    @Test
    public void testSyncReadAheadNotAlign() {
        DataBlockReadAccumulator accumulator = new DataBlockReadAccumulator();
        ObjectReaderLRUCache cache = Mockito.mock(ObjectReaderLRUCache.class);
        S3Operator s3Operator = Mockito.mock(S3Operator.class);
        ObjectManager objectManager = Mockito.mock(ObjectManager.class);
        BlockCache blockCache = Mockito.mock(BlockCache.class);
        Map<ReadAheadTaskKey, CompletableFuture<Void>> inflightReadAheadTasks = new HashMap<>();
        StreamReader streamReader = new StreamReader(s3Operator, objectManager, blockCache, cache, accumulator, inflightReadAheadTasks, new InflightReadThrottle());

        long startOffset = 32;
        StreamReader.ReadContext context = new StreamReader.ReadContext(startOffset, 256);
        ObjectReader.DataBlockIndex index1 = new ObjectReader.DataBlockIndex(0, 0, 256, 128);
        context.streamDataBlocksPair = List.of(
                new ImmutablePair<>(1L, List.of(
                        new StreamDataBlock(233L, 0, 128, 1, index1))));

        ObjectReader reader = Mockito.mock(ObjectReader.class);
        ObjectReader.DataBlock dataBlock1 = Mockito.mock(ObjectReader.DataBlock.class);
        StreamRecordBatch record1 = new StreamRecordBatch(233L, 0, 0, 64, TestUtils.random(128));
        record1.release();
        StreamRecordBatch record2 = new StreamRecordBatch(233L, 0, 64, 64, TestUtils.random(128));
        record2.release();
        List<StreamRecordBatch> records = List.of(record1, record2);
        AtomicInteger remaining = new AtomicInteger(0);
        Assertions.assertEquals(1, record1.getPayload().refCnt());
        Assertions.assertEquals(1, record2.getPayload().refCnt());
        Mockito.when(dataBlock1.iterator()).thenReturn(new CloseableIterator<>() {
            @Override
            public void close() {
            }

            @Override
            public boolean hasNext() {
                return remaining.get() < records.size();
            }

            @Override
            public StreamRecordBatch next() {
                if (!hasNext()) {
                    throw new IllegalStateException("no more elements");
                }
                return records.get(remaining.getAndIncrement());
            }
        });
        Mockito.when(reader.read(index1)).thenReturn(CompletableFuture.completedFuture(dataBlock1));
        context.objectReaderMap = new HashMap<>(Map.of(1L, reader));
        inflightReadAheadTasks.put(new ReadAheadTaskKey(233L, startOffset), new CompletableFuture<>());
        CompletableFuture<List<StreamRecordBatch>> cf = streamReader.handleSyncReadAhead(233L, startOffset,
                999, 64, Mockito.mock(ReadAheadAgent.class), UUID.randomUUID(), new TimerUtil(), context);

        cf.whenComplete((rst, ex) -> {
            Assertions.assertNull(ex);
            Assertions.assertTrue(inflightReadAheadTasks.isEmpty());
            Assertions.assertEquals(1, rst.size());
            Assertions.assertTrue(record1.equals(rst.get(0)));
            Assertions.assertEquals(2, record1.getPayload().refCnt());
            Assertions.assertEquals(1, record2.getPayload().refCnt());
        }).join();
    }

    @Test
    public void testSyncReadAheadException() {
        DataBlockReadAccumulator accumulator = new DataBlockReadAccumulator();
        ObjectReaderLRUCache cache = Mockito.mock(ObjectReaderLRUCache.class);
        S3Operator s3Operator = Mockito.mock(S3Operator.class);
        ObjectManager objectManager = Mockito.mock(ObjectManager.class);
        BlockCache blockCache = Mockito.mock(BlockCache.class);
        StreamReader streamReader = new StreamReader(s3Operator, objectManager, blockCache, cache, accumulator, new HashMap<>(), new InflightReadThrottle());

        StreamReader.ReadContext context = new StreamReader.ReadContext(0, 512);
        ObjectReader.DataBlockIndex index1 = new ObjectReader.DataBlockIndex(0, 0, 256, 128);
        ObjectReader.DataBlockIndex index2 = new ObjectReader.DataBlockIndex(1, 256, 256, 128);
        context.streamDataBlocksPair = List.of(
                new ImmutablePair<>(1L, List.of(
                        new StreamDataBlock(233L, 0, 128, 1, index1),
                        new StreamDataBlock(233L, 128, 256, 1, index2))));

        ObjectReader reader = Mockito.mock(ObjectReader.class);
        ObjectReader.DataBlock dataBlock1 = Mockito.mock(ObjectReader.DataBlock.class);
        StreamRecordBatch record1 = new StreamRecordBatch(233L, 0, 0, 64, TestUtils.random(128));
        record1.release();
        StreamRecordBatch record2 = new StreamRecordBatch(233L, 0, 0, 64, TestUtils.random(128));
        record2.release();
        List<StreamRecordBatch> records = List.of(record1, record2);
        AtomicInteger remaining = new AtomicInteger(records.size());
        Assertions.assertEquals(1, record1.getPayload().refCnt());
        Assertions.assertEquals(1, record2.getPayload().refCnt());
        Mockito.when(dataBlock1.iterator()).thenReturn(new CloseableIterator<>() {
            @Override
            public void close() {
            }

            @Override
            public boolean hasNext() {
                return remaining.get() > 0;
            }

            @Override
            public StreamRecordBatch next() {
                if (remaining.decrementAndGet() < 0) {
                    throw new IllegalStateException("no more elements");
                }
                return records.get(remaining.get());
            }
        });
        Mockito.when(reader.read(index1)).thenReturn(CompletableFuture.completedFuture(dataBlock1));
        Mockito.when(reader.read(index2)).thenReturn(CompletableFuture.failedFuture(new RuntimeException("exception")));
        context.objectReaderMap = new HashMap<>(Map.of(1L, reader));
        CompletableFuture<List<StreamRecordBatch>> cf = streamReader.handleSyncReadAhead(233L, 0,
                512, 1024, Mockito.mock(ReadAheadAgent.class), UUID.randomUUID(), new TimerUtil(), context);

        Threads.sleep(1000);

        try {
            cf.whenComplete((rst, ex) -> {
                Assertions.assertThrowsExactly(CompletionException.class, () -> {
                    throw ex;
                });
                Assertions.assertNull(rst);
                Assertions.assertEquals(1, record1.getPayload().refCnt());
                Assertions.assertEquals(1, record2.getPayload().refCnt());
            }).join();
        } catch (CompletionException e) {
            Assertions.assertEquals("exception", e.getCause().getMessage());
        }
    }

    @Test
    public void testAsyncReadAhead() {
        DataBlockReadAccumulator accumulator = new DataBlockReadAccumulator();
        ObjectReaderLRUCache cache = Mockito.mock(ObjectReaderLRUCache.class);
        S3Operator s3Operator = Mockito.mock(S3Operator.class);
        ObjectManager objectManager = Mockito.mock(ObjectManager.class);
        BlockCache blockCache = Mockito.mock(BlockCache.class);
        StreamReader streamReader = new StreamReader(s3Operator, objectManager, blockCache, cache, accumulator, new HashMap<>(), new InflightReadThrottle());

        StreamReader.ReadContext context = new StreamReader.ReadContext(0, 256);
        ObjectReader.DataBlockIndex index1 = new ObjectReader.DataBlockIndex(0, 0, 256, 128);
        context.streamDataBlocksPair = List.of(
                new ImmutablePair<>(1L, List.of(
                        new StreamDataBlock(233L, 0, 128, 1, index1))));

        ObjectReader reader = Mockito.mock(ObjectReader.class);
        ObjectReader.DataBlock dataBlock1 = Mockito.mock(ObjectReader.DataBlock.class);
        StreamRecordBatch record1 = new StreamRecordBatch(233L, 0, 0, 64, TestUtils.random(128));
        record1.release();
        StreamRecordBatch record2 = new StreamRecordBatch(233L, 0, 64, 64, TestUtils.random(128));
        record2.release();
        List<StreamRecordBatch> records = List.of(record1, record2);
        AtomicInteger remaining = new AtomicInteger(0);
        Assertions.assertEquals(1, record1.getPayload().refCnt());
        Assertions.assertEquals(1, record2.getPayload().refCnt());
        Mockito.when(dataBlock1.iterator()).thenReturn(new CloseableIterator<>() {
            @Override
            public void close() {
            }

            @Override
            public boolean hasNext() {
                return remaining.get() < records.size();
            }

            @Override
            public StreamRecordBatch next() {
                if (!hasNext()) {
                    throw new IllegalStateException("no more elements");
                }
                return records.get(remaining.getAndIncrement());
            }
        });
        Mockito.when(reader.read(index1)).thenReturn(CompletableFuture.completedFuture(dataBlock1));
        context.objectReaderMap = new HashMap<>(Map.of(1L, reader));

        CompletableFuture<Void> cf = streamReader.handleAsyncReadAhead(233L, 0, 999, 1024, Mockito.mock(ReadAheadAgent.class), new TimerUtil(), context);

        cf.whenComplete((rst, ex) -> {
            Assertions.assertNull(ex);
            Assertions.assertEquals(1, record1.getPayload().refCnt());
            Assertions.assertEquals(1, record2.getPayload().refCnt());
        }).join();
    }

    @Test
    public void testAsyncReadAheadException() {
        DataBlockReadAccumulator accumulator = new DataBlockReadAccumulator();
        ObjectReaderLRUCache cache = Mockito.mock(ObjectReaderLRUCache.class);
        S3Operator s3Operator = Mockito.mock(S3Operator.class);
        ObjectManager objectManager = Mockito.mock(ObjectManager.class);
        BlockCache blockCache = Mockito.mock(BlockCache.class);
        StreamReader streamReader = new StreamReader(s3Operator, objectManager, blockCache, cache, accumulator, new HashMap<>(), new InflightReadThrottle());

        StreamReader.ReadContext context = new StreamReader.ReadContext(0, 256);
        ObjectReader.DataBlockIndex index1 = new ObjectReader.DataBlockIndex(0, 0, 256, 128);
        ObjectReader.DataBlockIndex index2 = new ObjectReader.DataBlockIndex(1, 256, 256, 128);
        context.streamDataBlocksPair = List.of(
                new ImmutablePair<>(1L, List.of(
                        new StreamDataBlock(233L, 0, 128, 1, index1),
                        new StreamDataBlock(233L, 128, 256, 1, index2))));

        ObjectReader reader = Mockito.mock(ObjectReader.class);
        ObjectReader.DataBlock dataBlock1 = Mockito.mock(ObjectReader.DataBlock.class);
        StreamRecordBatch record1 = new StreamRecordBatch(233L, 0, 0, 64, TestUtils.random(128));
        record1.release();
        StreamRecordBatch record2 = new StreamRecordBatch(233L, 0, 64, 64, TestUtils.random(128));
        record2.release();
        List<StreamRecordBatch> records = List.of(record1, record2);
        AtomicInteger remaining = new AtomicInteger(0);
        Assertions.assertEquals(1, record1.getPayload().refCnt());
        Assertions.assertEquals(1, record2.getPayload().refCnt());
        Mockito.when(dataBlock1.iterator()).thenReturn(new CloseableIterator<>() {
            @Override
            public void close() {
            }

            @Override
            public boolean hasNext() {
                return remaining.get() < records.size();
            }

            @Override
            public StreamRecordBatch next() {
                if (!hasNext()) {
                    throw new IllegalStateException("no more elements");
                }
                return records.get(remaining.getAndIncrement());
            }
        });
        Mockito.when(reader.read(index1)).thenReturn(CompletableFuture.completedFuture(dataBlock1));
        Mockito.when(reader.read(index2)).thenReturn(CompletableFuture.failedFuture(new RuntimeException("exception")));
        context.objectReaderMap = new HashMap<>(Map.of(1L, reader));

        CompletableFuture<Void> cf = streamReader.handleAsyncReadAhead(233L, 0, 999, 1024, Mockito.mock(ReadAheadAgent.class), new TimerUtil(), context);

        try {
            cf.whenComplete((rst, ex) -> {
                Assertions.assertThrowsExactly(CompletionException.class, () -> {
                    throw ex;
                });
                Assertions.assertEquals(1, record1.getPayload().refCnt());
                Assertions.assertEquals(1, record2.getPayload().refCnt());
            }).join();
        } catch (CompletionException e) {
            Assertions.assertEquals("exception", e.getCause().getMessage());
        }

    }
}
