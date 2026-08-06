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

import com.automq.stream.api.ReadOptions;
import com.automq.stream.api.exceptions.ErrorCode;
import com.automq.stream.api.exceptions.StreamClientException;
import com.automq.stream.s3.cache.CacheAccessType;
import com.automq.stream.s3.cache.LogCache;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.context.FetchContext;
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
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.s3.wal.impl.MemoryWriteAheadLog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.automq.stream.s3.TestUtils.random;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@Tag("S3Unit")
public class S3StorageTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3StorageTest.class);

    StreamManager streamManager;
    ObjectManager objectManager;
    WriteAheadLog wal;
    ObjectStorage objectStorage;
    StorageFailureHandler storageFailureHandler;
    S3BlockCache blockCache;
    S3Storage storage;
    Config config;

    private static StreamRecordBatch newRecord(long streamId, long offset) {
        return newRecord(streamId, offset, 1);
    }

    private static StreamRecordBatch newRecord(long streamId, long offset, int count) {
        return StreamRecordBatch.of(streamId, 0, offset, count, random(1), DefaultByteBufSupplier.INSTANCE);
    }

    @BeforeEach
    public void setup() {
        config = new Config();
        config.blockCacheSize(0);
        objectManager = mock(ObjectManager.class);
        streamManager = mock(StreamManager.class);
        wal = spy(new MemoryWriteAheadLog());
        objectStorage = new MemoryObjectStorage();
        storageFailureHandler = mock(StorageFailureHandler.class);
        blockCache = mock(S3BlockCache.class);
        storage = new S3Storage(config, wal,
            streamManager, objectManager, blockCache, objectStorage, storageFailureHandler);
    }

    /**
     * Given historical object coverage is unavailable, when the requested range is fully present in the target-local
     * LogCache, then the read completes immediately with continuous records.
     */
    @Test
    public void testCompleteLocalLogCacheHitDoesNotWaitForHistoricalCoverage() throws Exception {
        CompletableFuture<ReadDataBlock> historicalCoverage = new CompletableFuture<>();
        Mockito.when(blockCache.read(any(), eq(233L), eq(10L), eq(12L), eq(1024)))
            .thenReturn(historicalCoverage);
        storage.append(newRecord(233L, 10L)).get();
        storage.append(newRecord(233L, 11L)).get();

        CompletableFuture<ReadDataBlock> read = storage.read(233L, 10L, 12L, 1024);

        assertTrue(read.isDone());
        assertEquals(List.of(10L, 11L), offsets(read.get()));
    }

    /**
     * Given only a target-local cache suffix, when the requested prefix lacks object coverage, then the read remains
     * pending and returns the prefix and suffix exactly once after coverage becomes available.
     */
    @Test
    public void testLogCacheSuffixWaitsForRequestedPrefix() throws Exception {
        CompletableFuture<ReadDataBlock> historicalCoverage = new CompletableFuture<>();
        Mockito.when(blockCache.read(any(), eq(233L), eq(10L), eq(11L), eq(1024)))
            .thenReturn(historicalCoverage);
        storage.append(newRecord(233L, 11L)).get();

        CompletableFuture<ReadDataBlock> read = storage.read(233L, 10L, 12L, 1024);

        assertFalse(read.isDone());
        historicalCoverage.complete(new ReadDataBlock(List.of(newRecord(233L, 10L)), CacheAccessType.BLOCK_CACHE_MISS));
        assertEquals(List.of(10L, 11L), offsets(read.get(1, TimeUnit.SECONDS)));
    }

    /**
     * Given ordinary and snapshot reads observe the same logical end ahead of object coverage, when coverage becomes
     * available, then both pending reads converge to the same records rather than an empty result.
     */
    @Test
    public void testOrdinaryAndSnapshotReadsConvergeAfterObjectCoverage() throws Exception {
        CompletableFuture<ReadDataBlock> ordinaryCoverage = new CompletableFuture<>();
        CompletableFuture<ReadDataBlock> snapshotCoverage = new CompletableFuture<>();
        Mockito.when(blockCache.read(any(), eq(233L), eq(10L), eq(12L), eq(1024)))
            .thenReturn(ordinaryCoverage)
            .thenReturn(snapshotCoverage);
        FetchContext snapshotContext = new FetchContext();
        snapshotContext.setReadOptions(ReadOptions.builder().snapshotRead(true).build());

        CompletableFuture<ReadDataBlock> ordinaryRead = storage.read(233L, 10L, 12L, 1024);
        CompletableFuture<ReadDataBlock> snapshotRead = storage.read(snapshotContext, 233L, 10L, 12L, 1024);

        assertFalse(ordinaryRead.isDone());
        assertFalse(snapshotRead.isDone());
        ordinaryCoverage.complete(coveredRange(233L, 10L, 12L));
        snapshotCoverage.complete(coveredRange(233L, 10L, 12L));
        assertEquals(List.of(10L, 11L), offsets(ordinaryRead.get(1, TimeUnit.SECONDS)));
        assertEquals(List.of(10L, 11L), offsets(snapshotRead.get(1, TimeUnit.SECONDS)));
    }

    /**
     * Given the requested trim point is a historical batch boundary and the read crosses into current-owner cache,
     * when historical coverage arrives, then batch boundaries are preserved without loss, duplication, or a hole.
     */
    @Test
    public void testReadAcrossTrimBatchAndOwnershipBoundary() throws Exception {
        CompletableFuture<ReadDataBlock> historicalCoverage = new CompletableFuture<>();
        Mockito.when(blockCache.read(any(), eq(233L), eq(12L), eq(13L), eq(1024)))
            .thenReturn(historicalCoverage);
        storage.append(newRecord(233L, 13L, 2)).get();

        CompletableFuture<ReadDataBlock> read = storage.read(233L, 12L, 15L, 1024);

        assertFalse(read.isDone());
        historicalCoverage.complete(new ReadDataBlock(List.of(newRecord(233L, 12L)),
            CacheAccessType.BLOCK_CACHE_MISS));
        ReadDataBlock result = read.get(1, TimeUnit.SECONDS);
        assertEquals(List.of(12L, 13L), offsets(result));
        assertEquals(List.of(1, 2), result.getRecords().stream()
            .map(StreamRecordBatch::getCount).collect(Collectors.toList()));
    }

    private static ReadDataBlock coveredRange(long streamId, long startOffset, long endOffset) {
        return new ReadDataBlock(java.util.stream.LongStream.range(startOffset, endOffset)
            .mapToObj(offset -> newRecord(streamId, offset)).collect(Collectors.toList()),
            CacheAccessType.BLOCK_CACHE_MISS);
    }

    private static List<Long> offsets(ReadDataBlock dataBlock) {
        return dataBlock.getRecords().stream().map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList());
    }

    @Test
    public void testAppend() throws Exception {
        Mockito.when(objectManager.prepareObject(eq(1), anyLong())).thenReturn(CompletableFuture.completedFuture(16L));
        CommitStreamSetObjectResponse resp = new CommitStreamSetObjectResponse();
        Mockito.when(objectManager.commitStreamSetObject(any())).thenReturn(CompletableFuture.completedFuture(resp));

        CompletableFuture<Void> cf1 = storage.append(
            StreamRecordBatch.of(233, 1, 10, 1, random(100), DefaultByteBufSupplier.INSTANCE)
        );
        CompletableFuture<Void> cf2 = storage.append(
            StreamRecordBatch.of(233, 1, 11, 2, random(100), DefaultByteBufSupplier.INSTANCE)
        );
        CompletableFuture<Void> cf3 = storage.append(
            StreamRecordBatch.of(234, 3, 100, 1, random(100), DefaultByteBufSupplier.INSTANCE)
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
    public void testAwaitStreamUpload() throws Exception {
        CompletableFuture<CommitStreamSetObjectResponse> commitFuture = new CompletableFuture<>();
        Mockito.when(objectManager.prepareObject(eq(1), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(16L));
        Mockito.when(objectManager.commitStreamSetObject(any())).thenReturn(commitFuture);
        storage.append(newRecord(233L, 10L)).get(1, TimeUnit.SECONDS);

        CompletableFuture<Void> upload = storage.forceUpload(233L);
        CompletableFuture<Void> uploadBarrier = storage.awaitUpload(233L);

        assertFalse(uploadBarrier.isDone());
        assertTrue(storage.awaitUpload(234L).isDone());
        verify(objectManager, timeout(1000)).commitStreamSetObject(any());

        commitFuture.complete(new CommitStreamSetObjectResponse());

        upload.get(1, TimeUnit.SECONDS);
        uploadBarrier.get(1, TimeUnit.SECONDS);
        assertTrue(storage.awaitUpload(233L).isDone());
    }

    /**
     * Given a fast-closed stream still has WAL data awaiting object commit, when storage shuts down, then shutdown
     * rejects later appends, waits for the commit, and preserves the WAL until the drain completes.
     */
    @Test
    public void testShutdownDrainsUploadsBeforeClosingWal() throws Exception {
        CompletableFuture<CommitStreamSetObjectResponse> commitFuture = new CompletableFuture<>();
        Mockito.when(objectManager.prepareObject(eq(1), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(16L));
        Mockito.when(objectManager.commitStreamSetObject(any())).thenReturn(commitFuture);
        storage.append(newRecord(233L, 10L)).get(1, TimeUnit.SECONDS);

        CompletableFuture<Void> shutdownFuture = CompletableFuture.runAsync(storage::shutdown);

        verify(objectManager, timeout(1000)).commitStreamSetObject(any());
        assertFalse(shutdownFuture.isDone());
        verify(wal, never()).shutdownGracefully();
        CompletableFuture<Void> rejectedAppend = storage.append(newRecord(233L, 11L));
        assertThrows(ExecutionException.class, () -> rejectedAppend.get(1, TimeUnit.SECONDS));

        commitFuture.complete(new CommitStreamSetObjectResponse());
        shutdownFuture.get(1, TimeUnit.SECONDS);
        verify(wal).shutdownGracefully();
    }

    /**
     * Given a committed upload still has a delayed WAL trim, when that trim fails during shutdown, then shutdown
     * suppresses the failure because replaying the untrimmed committed record is safe.
     */
    @Test
    public void testShutdownSuppressesPendingTrimFailure() throws Exception {
        config.snapshotReadEnable(true);
        S3Storage snapshotStorage = new S3Storage(config, wal,
            streamManager, objectManager, blockCache, objectStorage, mock(StorageFailureHandler.class));
        CompletableFuture<Void> trimFuture = new CompletableFuture<>();
        Mockito.when(objectManager.prepareObject(eq(1), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(16L));
        Mockito.when(objectManager.commitStreamSetObject(any()))
            .thenReturn(CompletableFuture.completedFuture(new CommitStreamSetObjectResponse()));
        Mockito.doReturn(trimFuture).when(wal).trim(any());
        snapshotStorage.append(newRecord(233L, 10L)).get(1, TimeUnit.SECONDS);

        CompletableFuture<Void> shutdownFuture = CompletableFuture.runAsync(snapshotStorage::shutdown);

        verify(wal, timeout(1000)).trim(any());
        assertFalse(shutdownFuture.isDone());
        verify(wal, never()).shutdownGracefully();

        trimFuture.completeExceptionally(new IOException("trim failed"));
        shutdownFuture.get(1, TimeUnit.SECONDS);
        verify(wal).shutdownGracefully();
    }

    /** Shutdown keeps the WAL open until a trim that has already started completes. */
    @Test
    public void testShutdownWaitsForInflightTrimBeforeClosingWal() throws Exception {
        CompletableFuture<Void> trimFuture = new CompletableFuture<>();
        Mockito.when(objectManager.prepareObject(eq(1), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(16L));
        Mockito.when(objectManager.commitStreamSetObject(any()))
            .thenReturn(CompletableFuture.completedFuture(new CommitStreamSetObjectResponse()));
        Mockito.doReturn(trimFuture).when(wal).trim(any());
        storage.append(newRecord(233L, 10L)).get(1, TimeUnit.SECONDS);

        CompletableFuture<Void> shutdownFuture = CompletableFuture.runAsync(storage::shutdown);

        verify(wal, timeout(1000)).trim(any());
        assertFalse(shutdownFuture.isDone());
        verify(wal, never()).shutdownGracefully();

        trimFuture.complete(null);
        shutdownFuture.get(1, TimeUnit.SECONDS);
        verify(wal).shutdownGracefully();
    }

    /**
     * Given an append was admitted before shutdown but backed off before entering the WAL, when storage shuts down,
     * then the accepted append enters the WAL and commits instead of being rejected at the shutdown boundary.
     */
    @Test
    public void testShutdownDrainsAcceptedBackoffAppend() throws Exception {
        Mockito.when(objectManager.prepareObject(eq(1), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(16L));
        Mockito.when(objectManager.commitStreamSetObject(any()))
            .thenReturn(CompletableFuture.completedFuture(new CommitStreamSetObjectResponse()));
        doThrow(new OverCapacityException("back off once")).doCallRealMethod().when(wal).append(any(), any());
        CompletableFuture<Void> appendFuture = storage.append(newRecord(233L, 10L));

        CompletableFuture<Void> shutdownFuture = CompletableFuture.runAsync(storage::shutdown);

        appendFuture.get(1, TimeUnit.SECONDS);
        shutdownFuture.get(1, TimeUnit.SECONDS);
        verify(objectManager).commitStreamSetObject(any());
        verify(wal).shutdownGracefully();
    }

    /** A newer owner fencing a recovered old epoch is a terminal per-stream cleanup result. */
    @Test
    public void testRecoveryAcceptsFencedCloseAfterOwnershipHandoff() throws Exception {
        WriteAheadLog recoveryWal = mock(WriteAheadLog.class);
        StreamMetadata historical = new StreamMetadata(233L, 1L, 0L, 10L, StreamState.OPENED);
        Mockito.when(streamManager.getOpeningStreams())
            .thenReturn(CompletableFuture.completedFuture(List.of(historical)));
        Mockito.when(streamManager.closeStream(233L, 1L))
            .thenReturn(CompletableFuture.failedFuture(
                new StreamClientException(ErrorCode.EXPIRED_STREAM_EPOCH, "new owner")));
        Mockito.when(recoveryWal.recover()).thenReturn(Collections.emptyIterator());
        Mockito.when(recoveryWal.reset()).thenReturn(CompletableFuture.completedFuture(null));

        assertDoesNotThrow(() -> storage.recover0(recoveryWal, streamManager, objectManager, LOGGER));

        verify(recoveryWal).reset();
    }

    /** Recovery must continue propagating close failures that do not prove the old epoch is terminal. */
    @Test
    public void testRecoveryPropagatesOtherCloseFailure() throws Exception {
        WriteAheadLog recoveryWal = mock(WriteAheadLog.class);
        StreamMetadata historical = new StreamMetadata(233L, 1L, 0L, 10L, StreamState.OPENED);
        Mockito.when(streamManager.getOpeningStreams())
            .thenReturn(CompletableFuture.completedFuture(List.of(historical)));
        Mockito.when(streamManager.closeStream(233L, 1L))
            .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("controller unavailable")));
        Mockito.when(recoveryWal.recover()).thenReturn(Collections.emptyIterator());
        Mockito.when(recoveryWal.reset()).thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException exception = assertThrows(ExecutionException.class,
            () -> storage.recover0(recoveryWal, streamManager, objectManager, LOGGER));

        assertInstanceOf(IllegalStateException.class, exception.getCause());
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
        logCacheBlock1.lastRecordOffset(DefaultRecordOffset.of(0, 10L, 0));
        CompletableFuture<Void> cf1 = storage.uploadDeltaWAL(logCacheBlock1);

        LogCache.LogCacheBlock logCacheBlock2 = new LogCache.LogCacheBlock(1024);
        logCacheBlock2.put(newRecord(233L, 20L));
        logCacheBlock2.put(newRecord(234L, 20L));
        logCacheBlock2.lastRecordOffset(DefaultRecordOffset.of(0, 20L, 0));
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

    /**
     * Given object preparation fails, when a WAL upload starts, then the upload future fails and storage failover runs.
     */
    @Test
    public void testUploadWALObject_prepareFailureTriggersStorageFailover() {
        RuntimeException prepareFailure = new RuntimeException("prepare failure");
        Mockito.when(objectManager.prepareObject(anyInt(), anyLong()))
            .thenReturn(CompletableFuture.failedFuture(prepareFailure));

        LogCache.LogCacheBlock logCacheBlock = new LogCache.LogCacheBlock(1024);
        logCacheBlock.put(newRecord(233L, 10L));
        logCacheBlock.lastRecordOffset(DefaultRecordOffset.of(0, 10L, 0));

        CompletableFuture<Void> uploadCf = storage.uploadDeltaWAL(logCacheBlock);

        assertThrows(ExecutionException.class, () -> uploadCf.get(1, TimeUnit.SECONDS));
        verify(storageFailureHandler, timeout(1000)).handle(any());
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

}
