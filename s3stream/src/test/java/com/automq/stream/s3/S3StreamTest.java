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

import com.automq.stream.api.FetchResult;
import com.automq.stream.api.OpenStreamOptions;
import com.automq.stream.api.ReadOptions;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.exceptions.StreamClientException;
import com.automq.stream.s3.cache.CacheAccessType;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.streams.StreamManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class S3StreamTest {
    Storage storage;
    StreamManager streamManager;
    S3Stream stream;

    @BeforeEach
    public void setup() {
        storage = mock(Storage.class);
        streamManager = mock(StreamManager.class);
        stream = S3Stream.create(233, 1, 100, 233, storage, streamManager);
    }

    @Test
    public void testFetch() throws Throwable {
        stream.confirmOffset.set(120L);
        Mockito.when(storage.read(any(), eq(233L), eq(110L), eq(120L), eq(100)))
            .thenReturn(CompletableFuture.completedFuture(newReadDataBlock(110, 115, 110)));
        FetchResult rst = stream.fetch(110, 120, 100).get(1, TimeUnit.SECONDS);
        assertEquals(1, rst.recordBatchList().size());
        assertEquals(110, rst.recordBatchList().get(0).baseOffset());
        assertEquals(115, rst.recordBatchList().get(0).lastOffset());
        assertEquals(CacheAccessType.DELTA_WAL_CACHE_HIT, rst.getCacheAccessType());

        // TODO: add fetch from WAL cache

        boolean isException = false;
        try {
            stream.fetch(120, 140, 100).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StreamClientException) {
                isException = true;
            }
        }
        Assertions.assertTrue(isException);
    }

    /**
     * Given a pending snapshot fetch, when a local fetch starts, then the default overload creates isolated contexts.
     */
    @Test
    public void testDefaultFetchContextsAreIsolated() throws Exception {
        S3Stream snapshotStream = S3Stream.create(234L, 1L, 100L, 120L, storage, streamManager,
            OpenStreamOptions.builder().readWriteMode(OpenStreamOptions.ReadWriteMode.SNAPSHOT_READ).build());
        CompletableFuture<ReadDataBlock> snapshotRead = new CompletableFuture<>();
        Mockito.when(storage.read(any(), eq(234L), eq(110L), eq(120L), eq(100))).thenReturn(snapshotRead);
        Mockito.when(storage.read(any(), eq(233L), eq(110L), eq(120L), eq(100)))
            .thenReturn(CompletableFuture.completedFuture(newReadDataBlock(110, 115, 110)));

        CompletableFuture<FetchResult> snapshotFetch = snapshotStream.fetch(110L, 120L, 100);
        FetchResult localFetch = stream.fetch(110L, 120L, 100).get(1, TimeUnit.SECONDS);

        ArgumentCaptor<FetchContext> snapshotContext = ArgumentCaptor.forClass(FetchContext.class);
        ArgumentCaptor<FetchContext> localContext = ArgumentCaptor.forClass(FetchContext.class);
        verify(storage).read(snapshotContext.capture(), eq(234L), eq(110L), eq(120L), eq(100));
        verify(storage).read(localContext.capture(), eq(233L), eq(110L), eq(120L), eq(100));
        assertFalse(ReadOptions.DEFAULT.snapshotRead());
        assertTrue(snapshotContext.getValue().readOptions().snapshotRead());
        assertFalse(localContext.getValue().readOptions().snapshotRead());
        assertNotSame(snapshotContext.getValue(), localContext.getValue());

        snapshotRead.complete(newReadDataBlock(110, 115, 110));
        snapshotFetch.get(1, TimeUnit.SECONDS);
        assertEquals(1, localFetch.recordBatchList().size());
    }

    /**
     * Given a V6 stream with a blocked force upload, when close drains existing work, then Controller fast close
     * completes with the broker append tail without waiting for ObjectStorage.
     */
    @Test
    public void testV6CloseDoesNotWaitForForceUpload() {
        CompletableFuture<Void> forceUpload = new CompletableFuture<>();
        when(streamManager.isFastCloseSupported()).thenReturn(true);
        when(storage.forceUpload(233L)).thenReturn(forceUpload);
        when(streamManager.closeStream(233L, 1L, 233L)).thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> close = stream.close();

        assertTrue(close.isDone());
        assertFalse(forceUpload.isDone());
        verify(streamManager).closeStream(233L, 1L, 233L);
    }

    /**
     * Given Controller opens the new owner at the source final append tail, when the target performs its first append,
     * then the new batch starts exactly at that authorized tail.
     */
    @Test
    public void testFirstTargetAppendStartsAtControllerAuthorizedTail() throws Exception {
        RecordBatch recordBatch = mock(RecordBatch.class);
        when(recordBatch.count()).thenReturn(2);
        when(recordBatch.rawPayload()).thenReturn(ByteBuffer.allocate(1));
        when(storage.append(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        long firstTargetBaseOffset = stream.append(recordBatch).get().baseOffset();

        assertEquals(233L, firstTargetBaseOffset);
        assertEquals(235L, stream.nextOffset());
    }

    /**
     * Given a V5 stream with a blocked force upload, when close starts, then legacy Controller close waits until
     * the upload completes.
     */
    @Test
    public void testV5CloseWaitsForForceUploadBeforeLegacyClose() {
        CompletableFuture<Void> forceUpload = new CompletableFuture<>();
        when(storage.forceUpload(233L)).thenReturn(forceUpload);
        when(streamManager.closeStream(233L, 1L)).thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> close = stream.close();

        assertFalse(close.isDone());
        verify(streamManager, never()).closeStream(233L, 1L);

        forceUpload.complete(null);

        assertTrue(close.isDone());
        verify(streamManager).closeStream(233L, 1L);
    }

    /**
     * Given pending append work on a V6 stream, when close starts, then force upload and Controller close start only
     * after the append drains and use the resulting append tail.
     */
    @Test
    public void testV6ClosePreservesAppendDrain() {
        CompletableFuture<Void> append = new CompletableFuture<>();
        RecordBatch recordBatch = mock(RecordBatch.class);
        when(recordBatch.count()).thenReturn(1);
        when(recordBatch.rawPayload()).thenReturn(ByteBuffer.allocate(1));
        when(storage.append(any(), any())).thenReturn(append);
        when(streamManager.isFastCloseSupported()).thenReturn(true);
        when(storage.forceUpload(233L)).thenReturn(new CompletableFuture<>());
        when(streamManager.closeStream(233L, 1L, 234L)).thenReturn(CompletableFuture.completedFuture(null));

        stream.append(recordBatch);
        CompletableFuture<Void> close = stream.close();

        assertFalse(close.isDone());
        verify(storage, never()).forceUpload(233L);
        verify(streamManager, never()).closeStream(233L, 1L, 234L);

        append.complete(null);

        assertTrue(close.isDone());
        verify(storage).forceUpload(233L);
        verify(streamManager).closeStream(233L, 1L, 234L);
    }

    /**
     * Given pending trim work on a V6 stream, when close starts, then force upload and Controller close start only
     * after the trim drains.
     */
    @Test
    public void testV6ClosePreservesTrimDrain() {
        CompletableFuture<Void> trim = new CompletableFuture<>();
        when(streamManager.trimStream(233L, 1L, 150L)).thenReturn(trim);
        when(streamManager.isFastCloseSupported()).thenReturn(true);
        when(storage.forceUpload(233L)).thenReturn(new CompletableFuture<>());
        when(streamManager.closeStream(233L, 1L, 233L)).thenReturn(CompletableFuture.completedFuture(null));

        stream.trim(150L);
        CompletableFuture<Void> close = stream.close();

        assertFalse(close.isDone());
        verify(storage, never()).forceUpload(233L);
        verify(streamManager, never()).closeStream(233L, 1L, 233L);

        trim.complete(null);

        assertTrue(close.isDone());
        verify(storage).forceUpload(233L);
        verify(streamManager).closeStream(233L, 1L, 233L);
    }

    /**
     * Given a V6 force upload fails, when Controller fast close succeeds, then close succeeds independently and the
     * source remains closed to new appends.
     */
    @Test
    public void testV6ForceUploadFailureDoesNotFailCloseAndAppendRemainsRejected() {
        RuntimeException uploadFailure = new RuntimeException("upload failed");
        when(streamManager.isFastCloseSupported()).thenReturn(true);
        when(storage.forceUpload(233L)).thenReturn(CompletableFuture.failedFuture(uploadFailure));
        when(streamManager.closeStream(233L, 1L, 233L)).thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> close = stream.close();

        close.join();
        assertThrows(ExecutionException.class, () -> stream.append(mock(RecordBatch.class)).get());
    }

    /**
     * Given V6 Controller close fails while upload remains in progress, when the source is closing, then the failure
     * reaches the close caller and the source remains closed to new appends.
     */
    @Test
    public void testV6ControllerCloseFailureIsVisibleAndAppendRemainsRejected() {
        RuntimeException closeFailure = new RuntimeException("close failed");
        when(streamManager.isFastCloseSupported()).thenReturn(true);
        when(storage.forceUpload(233L)).thenReturn(new CompletableFuture<>());
        when(streamManager.closeStream(233L, 1L, 233L)).thenReturn(CompletableFuture.failedFuture(closeFailure));

        CompletableFuture<Void> close = stream.close();

        ExecutionException closeException = assertThrows(ExecutionException.class, close::get);
        assertEquals(closeFailure, closeException.getCause());
        assertThrows(ExecutionException.class, () -> stream.append(mock(RecordBatch.class)).get());
    }

    @Test
    public void testPendingRequestTrackerCalculatesOldestPendingAge() {
        long[] now = {100L};
        PendingRequestTracker tracker = new PendingRequestTracker(() -> now[0]);
        CompletableFuture<Void> older = new CompletableFuture<>();
        CompletableFuture<Void> newer = new CompletableFuture<>();
        tracker.track(older, 10L);
        tracker.track(newer, 80L);

        assertEquals(90L, tracker.maxPendingLatencyNanos());
        Assertions.assertTrue(tracker.hasPendingOlderThan(90L));
        Assertions.assertFalse(tracker.hasPendingOlderThan(91L));

        newer.complete(null);
        assertEquals(90L, tracker.maxPendingLatencyNanos());
        older.complete(null);
        assertEquals(0L, tracker.maxPendingLatencyNanos());
    }

    ReadDataBlock newReadDataBlock(long start, long end, int size) {
        StreamRecordBatch record = StreamRecordBatch.of(0, 0, start, (int) (end - start), TestUtils.random(size), DefaultByteBufSupplier.INSTANCE);
        return new ReadDataBlock(List.of(record), CacheAccessType.DELTA_WAL_CACHE_HIT);
    }
}
