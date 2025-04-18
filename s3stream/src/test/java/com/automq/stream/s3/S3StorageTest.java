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

package com.automq.stream.s3;

import com.automq.stream.s3.cache.LogCache;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.cache.blockcache.DefaultObjectReaderFactory;
import com.automq.stream.s3.cache.blockcache.StreamReaders;
import com.automq.stream.s3.failover.StorageFailureHandler;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.CommitStreamSetObjectResponse;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.MemoryWriteAheadLog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;

import static com.automq.stream.s3.TestUtils.random;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@Tag("S3Unit")
public class S3StorageTest {
    StreamManager streamManager;
    ObjectManager objectManager;
    WriteAheadLog wal;
    ObjectStorage objectStorage;
    S3Storage storage;
    Config config;

    private static StreamRecordBatch newRecord(long streamId, long offset) {
        return new StreamRecordBatch(streamId, 0, offset, 1, random(1));
    }

    @BeforeEach
    public void setup() {
        config = new Config();
        config.blockCacheSize(0);
        objectManager = mock(ObjectManager.class);
        streamManager = mock(StreamManager.class);
        wal = spy(new MemoryWriteAheadLog());
        objectStorage = new MemoryObjectStorage();
        storage = new S3Storage(config, wal,
            streamManager, objectManager, new StreamReaders(config.blockCacheSize(), objectManager, objectStorage,
            new DefaultObjectReaderFactory(objectStorage)), objectStorage, mock(StorageFailureHandler.class));
    }

    @Test
    public void testAppend() throws Exception {
        Mockito.when(objectManager.prepareObject(eq(1), anyLong())).thenReturn(CompletableFuture.completedFuture(16L));
        CommitStreamSetObjectResponse resp = new CommitStreamSetObjectResponse();
        Mockito.when(objectManager.commitStreamSetObject(any())).thenReturn(CompletableFuture.completedFuture(resp));

        CompletableFuture<Void> cf1 = storage.append(
            new StreamRecordBatch(233, 1, 10, 1, random(100))
        );
        CompletableFuture<Void> cf2 = storage.append(
            new StreamRecordBatch(233, 1, 11, 2, random(100))
        );
        CompletableFuture<Void> cf3 = storage.append(
            new StreamRecordBatch(234, 3, 100, 1, random(100))
        );

        cf1.get(3, TimeUnit.SECONDS);
        cf2.get(3, TimeUnit.SECONDS);
        cf3.get(3, TimeUnit.SECONDS);

        ReadDataBlock readRst = storage.read(233, 10, 13, 90).get();
        assertEquals(1, readRst.getRecords().size());
        readRst = storage.read(233, 10, 13, 200).get();
        assertEquals(2, readRst.getRecords().size());

        storage.forceUpload(233L).get();
        ArgumentCaptor<CommitStreamSetObjectRequest> commitArg = ArgumentCaptor.forClass(CommitStreamSetObjectRequest.class);
        verify(objectManager).commitStreamSetObject(commitArg.capture());
        CommitStreamSetObjectRequest commitReq = commitArg.getValue();
        assertEquals(16L, commitReq.getObjectId());
        List<ObjectStreamRange> streamRanges = commitReq.getStreamRanges();
        assertEquals(2, streamRanges.size());
        assertEquals(233, streamRanges.get(0).getStreamId());
        assertEquals(10, streamRanges.get(0).getStartOffset());
        assertEquals(13, streamRanges.get(0).getEndOffset());
        assertEquals(234, streamRanges.get(1).getStreamId());
        assertEquals(100, streamRanges.get(1).getStartOffset());
        assertEquals(101, streamRanges.get(1).getEndOffset());
    }

    @Test
    public void testWALConfirmOffsetCalculator() {
        S3Storage.WALConfirmOffsetCalculator calc = new S3Storage.WALConfirmOffsetCalculator();
        WalWriteRequest r0 = new WalWriteRequest(null, 0L, null);
        WalWriteRequest r1 = new WalWriteRequest(null, 1L, null);
        WalWriteRequest r2 = new WalWriteRequest(null, 2L, null);
        WalWriteRequest r3 = new WalWriteRequest(null, 3L, null);

        calc.add(r3);
        calc.add(r1);
        calc.add(r2);
        calc.add(r0);

        calc.update();
        assertEquals(-1L, calc.get());

        r0.confirmed = true;
        calc.update();
        assertEquals(0L, calc.get());

        r3.confirmed = true;
        calc.update();
        assertEquals(0L, calc.get());

        r1.confirmed = true;
        calc.update();
        assertEquals(1L, calc.get());

        r2.confirmed = true;
        calc.update();
        assertEquals(3L, calc.get());
    }

    @Test
    public void testWALCallbackSequencer() {
        S3Storage.WALCallbackSequencer seq = new S3Storage.WALCallbackSequencer();
        WalWriteRequest r0 = new WalWriteRequest(newRecord(233L, 10L), 100L, new CompletableFuture<>());
        WalWriteRequest r1 = new WalWriteRequest(newRecord(233L, 11L), 101L, new CompletableFuture<>());
        WalWriteRequest r2 = new WalWriteRequest(newRecord(234L, 20L), 102L, new CompletableFuture<>());
        WalWriteRequest r3 = new WalWriteRequest(newRecord(234L, 21L), 103L, new CompletableFuture<>());

        seq.before(r0);
        seq.before(r1);
        seq.before(r2);
        seq.before(r3);

        assertEquals(Collections.emptyList(), seq.after(r3));
        assertEquals(List.of(r2, r3), seq.after(r2));
        assertEquals(List.of(r0), seq.after(r0));
        assertEquals(List.of(r1), seq.after(r1));
    }

    @Test
    public void testUploadWALObject_sequence() throws ExecutionException, InterruptedException, TimeoutException {
        List<CompletableFuture<Long>> objectIdCfList = List.of(new CompletableFuture<>(), new CompletableFuture<>());
        AtomicInteger objectCfIndex = new AtomicInteger();
        Mockito.doAnswer(invocation -> objectIdCfList.get(objectCfIndex.getAndIncrement())).when(objectManager).prepareObject(ArgumentMatchers.anyInt(), anyLong());

        List<CompletableFuture<CommitStreamSetObjectResponse>> commitCfList = List.of(new CompletableFuture<>(), new CompletableFuture<>());
        AtomicInteger commitCfIndex = new AtomicInteger();
        Mockito.doAnswer(invocation -> commitCfList.get(commitCfIndex.getAndIncrement())).when(objectManager).commitStreamSetObject(any());

        LogCache.LogCacheBlock logCacheBlock1 = new LogCache.LogCacheBlock(1024);
        logCacheBlock1.put(newRecord(233L, 10L));
        logCacheBlock1.put(newRecord(234L, 10L));
        logCacheBlock1.confirmOffset(10L);
        CompletableFuture<Void> cf1 = storage.uploadDeltaWAL(logCacheBlock1);

        LogCache.LogCacheBlock logCacheBlock2 = new LogCache.LogCacheBlock(1024);
        logCacheBlock2.put(newRecord(233L, 20L));
        logCacheBlock2.put(newRecord(234L, 20L));
        logCacheBlock2.confirmOffset(20L);
        CompletableFuture<Void> cf2 = storage.uploadDeltaWAL(logCacheBlock2);

        // sequence get objectId
        verify(objectManager, Mockito.timeout(1000).times(1)).prepareObject(ArgumentMatchers.anyInt(), anyLong());

        objectIdCfList.get(0).complete(1L);
        // trigger next upload prepare objectId
        verify(objectManager, Mockito.timeout(1000).times(2)).prepareObject(ArgumentMatchers.anyInt(), anyLong());
        verify(objectManager, Mockito.timeout(1000).times(1)).commitStreamSetObject(any());

        objectIdCfList.get(1).complete(2L);
        Thread.sleep(10);
        verify(objectManager, Mockito.times(1)).commitStreamSetObject(any());

        commitCfList.get(0).complete(new CommitStreamSetObjectResponse());
        verify(objectManager, Mockito.timeout(1000).times(2)).commitStreamSetObject(any());
        commitCfList.get(1).complete(new CommitStreamSetObjectResponse());
        cf1.get(1, TimeUnit.SECONDS);
        cf2.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testRecoverContinuousRecords() {
        List<RecoverResult> recoverResults = List.of(
            new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(233L, 10L))),
            new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(233L, 11L))),
            new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(233L, 12L))),
            new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(233L, 15L))),
            new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(234L, 20L)))
        );

        List<StreamMetadata> openingStreams = List.of(new StreamMetadata(233L, 0L, 0L, 11L, StreamState.OPENED));
        LogCache.LogCacheBlock cacheBlock = S3Storage.recoverContinuousRecords(recoverResults.iterator(), openingStreams);
        // ignore closed stream and noncontinuous records.
        assertEquals(1, cacheBlock.records().size());
        List<StreamRecordBatch> streamRecords = cacheBlock.records().get(233L);
        assertEquals(2, streamRecords.size());
        assertEquals(11L, streamRecords.get(0).getBaseOffset());
        assertEquals(12L, streamRecords.get(1).getBaseOffset());

        // simulate data loss
        openingStreams = List.of(
            new StreamMetadata(233L, 0L, 0L, 5L, StreamState.OPENED));
        boolean exception = false;
        try {
            S3Storage.recoverContinuousRecords(recoverResults.iterator(), openingStreams);
        } catch (IllegalStateException e) {
            exception = true;
        }
        Assertions.assertTrue(exception);
    }

    @Test
    public void testRecoverOutOfOrderRecords() {
        List<RecoverResult> recoverResults = List.of(
            new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(42L, 9L))),
            new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(42L, 10L))),
            new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(42L, 13L))),
            new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(42L, 11L))),
            new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(42L, 12L))),
            new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(42L, 14L))),
            new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(42L, 20L)))
        );

        List<StreamMetadata> openingStreams = List.of(new StreamMetadata(42L, 0L, 0L, 10L, StreamState.OPENED));
        LogCache.LogCacheBlock cacheBlock = S3Storage.recoverContinuousRecords(recoverResults.iterator(), openingStreams);
        // ignore closed stream and noncontinuous records.
        assertEquals(1, cacheBlock.records().size());
        List<StreamRecordBatch> streamRecords = cacheBlock.records().get(42L);
        assertEquals(5, streamRecords.size());
        assertEquals(10L, streamRecords.get(0).getBaseOffset());
        assertEquals(11L, streamRecords.get(1).getBaseOffset());
        assertEquals(12L, streamRecords.get(2).getBaseOffset());
        assertEquals(13L, streamRecords.get(3).getBaseOffset());
        assertEquals(14L, streamRecords.get(4).getBaseOffset());
    }

    @Test
    public void testWALOverCapacity() throws OverCapacityException {
        storage.append(newRecord(233L, 10L));
        storage.append(newRecord(233L, 11L));
        doThrow(new OverCapacityException("test")).when(wal).append(any(), any());

        Mockito.when(objectManager.prepareObject(eq(1), anyLong())).thenReturn(CompletableFuture.completedFuture(16L));
        CommitStreamSetObjectResponse resp = new CommitStreamSetObjectResponse();
        Mockito.when(objectManager.commitStreamSetObject(any())).thenReturn(CompletableFuture.completedFuture(resp));

        storage.append(newRecord(233L, 12L));

        ArgumentCaptor<CommitStreamSetObjectRequest> commitArg = ArgumentCaptor.forClass(CommitStreamSetObjectRequest.class);
        verify(objectManager, timeout(1000L).times(1)).commitStreamSetObject(commitArg.capture());
        CommitStreamSetObjectRequest commitRequest = commitArg.getValue();
        assertEquals(1, commitRequest.getStreamObjects().size());
        assertEquals(0, commitRequest.getStreamRanges().size());
        StreamObject range = commitRequest.getStreamObjects().get(0);
        assertEquals(233L, range.getStreamId());
        assertEquals(10L, range.getStartOffset());
        assertEquals(12L, range.getEndOffset());
    }

    static class TestRecoverResult implements RecoverResult {
        private final ByteBuf record;

        public TestRecoverResult(ByteBuf record) {
            this.record = record;
        }

        @Override
        public ByteBuf record() {
            return record;
        }

        @Override
        public long recordOffset() {
            return 0;
        }
    }
}
