/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    /**
     * WALCallbackSequencer - Test after() when a stream's queue becomes empty
     */
    @Test
    public void testAfterWhenQueueBecomesEmpty() {
        S3Storage.WALCallbackSequencer seq = new S3Storage.WALCallbackSequencer();
        long streamId = 700L;

        WalWriteRequest request = new WalWriteRequest(newRecord(streamId, 70L), 700L, new CompletableFuture<>());
        seq.before(request);

        // Process the request, making the queue empty
        List<WalWriteRequest> result = seq.after(request);
        assertEquals(List.of(request), result);

        // Verify that subsequent processing works correctly after the queue became empty
        // Add a new request to the same stream
        WalWriteRequest newRequest = new WalWriteRequest(newRecord(streamId, 71L), 701L, new CompletableFuture<>());
        seq.before(newRequest);

        // Process the new request
        List<WalWriteRequest> newResult = seq.after(newRequest);
        assertEquals(List.of(newRequest), newResult);
    }

    /**
     * WALCallbackSequencer
     */
    @Test
    public void testTryFreeWithEmptyQueue() {
        S3Storage.WALCallbackSequencer seq = new S3Storage.WALCallbackSequencer();
        long streamId = 200L;

        // Create a request and process it, emptying the queue
        WalWriteRequest request = new WalWriteRequest(newRecord(streamId, 20L), 200L, new CompletableFuture<>());
        seq.before(request);
        seq.after(request);

        // Call tryFree to remove the empty queue
        seq.tryFree(streamId);

        // If the queue was removed correctly, a new queue will be created and processed normally
        WalWriteRequest newRequest = new WalWriteRequest(newRecord(streamId, 21L), 201L, new CompletableFuture<>());
        seq.before(newRequest);
        List<WalWriteRequest> result = seq.after(newRequest);
        assertEquals(List.of(newRequest), result);
    }

    /**
     * WALCallbackSequencer
     */
    @Test
    public void testTryFreeWithNonEmptyQueue() {
        S3Storage.WALCallbackSequencer seq = new S3Storage.WALCallbackSequencer();
        long streamId = 300L;

        // Add multiple requests
        WalWriteRequest r0 = new WalWriteRequest(newRecord(streamId, 30L), 300L, new CompletableFuture<>());
        WalWriteRequest r1 = new WalWriteRequest(newRecord(streamId, 31L), 301L, new CompletableFuture<>());

        seq.before(r0);
        seq.before(r1);

        // Only process the first request, the queue should be non-empty
        seq.after(r0);

        // Call tryFree, but the queue should not be removed
        seq.tryFree(streamId);

        // The second request should still in the queue
        List<WalWriteRequest> result = seq.after(r1);
        assertEquals(List.of(r1), result);
    }

    /**
     * WALCallbackSequencer
     */
    @Test
    public void testBeforeExceptionHandling() {
        S3Storage.WALCallbackSequencer seq = new S3Storage.WALCallbackSequencer();

        // Create a request that will cause the before method to throw an exception,
        // For example, let record be null
        WalWriteRequest request = new WalWriteRequest(null, 500L, new CompletableFuture<>());

        seq.before(request);

        // Verify that the future has been completed abnormally
        assertTrue(request.cf.isCompletedExceptionally());
    }

    /**
     * WALCallbackSequencer
     */
    @Test
    public void testAfterWithDifferentOffset() {
        S3Storage.WALCallbackSequencer seq = new S3Storage.WALCallbackSequencer();
        long streamId = 600L;

        WalWriteRequest r0 = new WalWriteRequest(newRecord(streamId, 60L), 600L, new CompletableFuture<>());
        seq.before(r0);

        // Create a request with the same streamId but a different offset
        WalWriteRequest r1 = new WalWriteRequest(newRecord(streamId, 61L), 601L, new CompletableFuture<>());

        // Process r1, but it is not in the queue, so after should return an empty list
        List<WalWriteRequest> result = seq.after(r1);
        assertEquals(Collections.emptyList(), result);

        // Verify that r1 is marked as persistent
        assertTrue(r1.persisted);
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
