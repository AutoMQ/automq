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
import com.automq.stream.s3.ObjectWriter;
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.cache.DefaultS3BlockCache.ReadAheadTaskKey;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.MemoryS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.trace.context.TraceContext;
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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

public class StreamReaderTest {

    @Test
    public void testGetDataBlockIndices() {
        S3Operator s3Operator = new MemoryS3Operator();
        ObjectManager objectManager = Mockito.mock(ObjectManager.class);
        ObjectWriter objectWriter = ObjectWriter.writer(0, s3Operator, 1024, 1024);
        objectWriter.write(233, List.of(
                newRecord(233, 10, 5, 512),
                newRecord(233, 15, 10, 512)
        ));
        objectWriter.close();
        ObjectWriter objectWriter2 = ObjectWriter.writer(1, s3Operator, 1024, 1024);
        objectWriter2.write(233, List.of(
                newRecord(233, 25, 5, 512),
                newRecord(233, 30, 10, 512)
        ));
        objectWriter2.close();

        S3ObjectMetadata metadata1 = new S3ObjectMetadata(0, objectWriter.size(), S3ObjectType.STREAM);
        S3ObjectMetadata metadata2 = new S3ObjectMetadata(1, objectWriter2.size(), S3ObjectType.STREAM);

        doAnswer(invocation -> CompletableFuture.completedFuture(List.of(metadata1, metadata2)))
                .when(objectManager).getObjects(eq(233L), eq(15L), eq(1024L), eq(2));

        StreamReader streamReader = new StreamReader(s3Operator, objectManager, Mockito.mock(BlockCache.class), new HashMap<>(), new InflightReadThrottle());
        StreamReader.ReadContext context = new StreamReader.ReadContext(15L, 1024);
        streamReader.getDataBlockIndices(TraceContext.DEFAULT, 233L, 1024L, context).thenAccept(v -> {
            Assertions.assertEquals(40L, context.nextStartOffset);
            Assertions.assertEquals(0, context.nextMaxBytes);
            Assertions.assertEquals(2, context.streamDataBlocksPair.size());
        }).join();

    }

    private StreamRecordBatch newRecord(long streamId, long offset, int count, int payloadSize) {
        return new StreamRecordBatch(streamId, 0, offset, count, TestUtils.random(payloadSize));
    }

    @Test
    public void testSyncReadAheadInflight() {
        DataBlockReadAccumulator accumulator = new DataBlockReadAccumulator();
        ObjectReaderLRUCache cache = Mockito.mock(ObjectReaderLRUCache.class);
        S3Operator s3Operator = Mockito.mock(S3Operator.class);
        ObjectManager objectManager = Mockito.mock(ObjectManager.class);
        BlockCache blockCache = Mockito.mock(BlockCache.class);
        Map<DefaultS3BlockCache.ReadAheadTaskKey, DefaultS3BlockCache.ReadAheadTaskContext> inflightReadAheadTasks = new HashMap<>();
        StreamReader streamReader = Mockito.spy(new StreamReader(s3Operator, objectManager, blockCache, cache, accumulator, inflightReadAheadTasks, new InflightReadThrottle()));

        long streamId = 233L;
        long startOffset = 70;
        long endOffset = 1024;
        int maxBytes = 64;
        long objectId = 1;
        S3ObjectMetadata metadata = new S3ObjectMetadata(objectId, -1, S3ObjectType.STREAM);
        doAnswer(invocation -> CompletableFuture.completedFuture(List.of(metadata)))
                        .when(objectManager).getObjects(eq(streamId), eq(startOffset), anyLong(), anyInt());

        ObjectReader reader = Mockito.mock(ObjectReader.class);
        ObjectReader.DataBlockIndex index1 = new ObjectReader.DataBlockIndex(0, 0, 256, 128);
        doReturn(reader).when(streamReader).getObjectReader(metadata);
        doAnswer(invocation -> CompletableFuture.completedFuture(new ObjectReader.FindIndexResult(true, -1, -1,
                List.of(new StreamDataBlock(streamId, 64, 128, objectId, index1))))).when(reader).find(eq(streamId), eq(startOffset), anyLong(), eq(maxBytes));
        doReturn(new CompletableFuture<>()).when(reader).read(index1);

        streamReader.syncReadAhead(TraceContext.DEFAULT, streamId, startOffset, endOffset, maxBytes, Mockito.mock(ReadAheadAgent.class), UUID.randomUUID());
        Threads.sleep(1000);
        Assertions.assertEquals(2, inflightReadAheadTasks.size());
        ReadAheadTaskKey key1 = new ReadAheadTaskKey(233L, startOffset);
        ReadAheadTaskKey key2 = new ReadAheadTaskKey(233L, 64);
        Assertions.assertTrue(inflightReadAheadTasks.containsKey(key1));
        Assertions.assertTrue(inflightReadAheadTasks.containsKey(key2));
        Assertions.assertEquals(DefaultS3BlockCache.ReadBlockCacheStatus.WAIT_FETCH_DATA, inflightReadAheadTasks.get(key1).status);
        Assertions.assertEquals(DefaultS3BlockCache.ReadBlockCacheStatus.WAIT_FETCH_DATA, inflightReadAheadTasks.get(key2).status);
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
        CompletableFuture<List<StreamRecordBatch>> cf = streamReader.handleSyncReadAhead(TraceContext.DEFAULT,233L, 0,
                999, 64, Mockito.mock(ReadAheadAgent.class), UUID.randomUUID(), new TimerUtil(), context);

        cf.whenComplete((rst, ex) -> {
            Assertions.assertNull(ex);
            Assertions.assertEquals(1, rst.size());
            Assertions.assertEquals(record1, rst.get(0));
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
        Map<DefaultS3BlockCache.ReadAheadTaskKey, DefaultS3BlockCache.ReadAheadTaskContext> inflightReadAheadTasks = new HashMap<>();
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
        ReadAheadTaskKey key = new ReadAheadTaskKey(233L, startOffset);
        context.taskKeySet.add(key);
        inflightReadAheadTasks.put(key, new DefaultS3BlockCache.ReadAheadTaskContext(new CompletableFuture<>(), DefaultS3BlockCache.ReadBlockCacheStatus.INIT));
        CompletableFuture<List<StreamRecordBatch>> cf = streamReader.handleSyncReadAhead(TraceContext.DEFAULT, 233L, startOffset,
                999, 64, Mockito.mock(ReadAheadAgent.class), UUID.randomUUID(), new TimerUtil(), context);

        cf.whenComplete((rst, ex) -> {
            Assertions.assertNull(ex);
            Assertions.assertTrue(inflightReadAheadTasks.isEmpty());
            Assertions.assertEquals(1, rst.size());
            Assertions.assertEquals(record1, rst.get(0));
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
        CompletableFuture<List<StreamRecordBatch>> cf = streamReader.handleSyncReadAhead(TraceContext.DEFAULT, 233L, 0,
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
