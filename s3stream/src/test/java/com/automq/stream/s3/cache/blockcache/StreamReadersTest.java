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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class StreamReadersTest {
    private static final long STREAM_ID_1 = 100L;
    private static final long STREAM_ID_2 = 200L;
    private static final int BLOCK_SIZE_THRESHOLD = 1024;

    private Map<Long, MockObject> objects;
    private ObjectManager objectManager;
    private ObjectStorage objectStorage;
    private ObjectReaderFactory objectReaderFactory;
    private StreamReaders streamReaders;
    private MockTime mockTime;
    private ScheduledExecutorService retryScheduler;
    private CompletableFuture<Runnable> retryTask;

    @BeforeEach
    void setup() {
        objects = new HashMap<>();

        // Create mock objects for testing with different offset ranges
        // Object 1: STREAM_ID_1 offset 0-2
        objects.put(1L, MockObject.builder(1L, BLOCK_SIZE_THRESHOLD).write(STREAM_ID_1, List.of(
            StreamRecordBatch.of(STREAM_ID_1, 0, 0, 2, TestUtils.random(100), DefaultByteBufSupplier.INSTANCE)
        )).build());
        // Object 2: STREAM_ID_2 offset 0-1
        objects.put(2L, MockObject.builder(2L, BLOCK_SIZE_THRESHOLD).write(STREAM_ID_2, List.of(
            StreamRecordBatch.of(STREAM_ID_2, 0, 0, 1, TestUtils.random(100), DefaultByteBufSupplier.INSTANCE)
        )).build());

        objectManager = mock(ObjectManager.class);

        when(objectManager.isObjectExist(anyLong())).thenReturn(true);
        // Mock getObjects method to return appropriate objects based on offset ranges
        // For STREAM_ID_1, use the combined object that covers 0-2 range
        when(objectManager.getObjects(eq(STREAM_ID_1), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(List.of(objects.get(1L).metadata)));
        // STREAM_ID_2 offset 0-1 -> object 3
        when(objectManager.getObjects(eq(STREAM_ID_2), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(List.of(objects.get(2L).metadata)));

        objectStorage = new MemoryObjectStorage();

        objectReaderFactory = new ObjectReaderFactory() {
            @Override
            public ObjectReader get(S3ObjectMetadata metadata) {
                return objects.get(metadata.objectId()).objectReader();
            }

            @Override
            public ObjectStorage getObjectStorage() {
                return objectStorage;
            }
        };

        mockTime = new MockTime();
        retryScheduler = mock(ScheduledExecutorService.class);
        retryTask = new CompletableFuture<>();
        doAnswer(invocation -> {
            retryTask.complete(invocation.getArgument(0));
            return null;
        }).when(retryScheduler).schedule(any(Runnable.class), eq(10L), eq(TimeUnit.SECONDS));
        streamReaders = new StreamReaders(Long.MAX_VALUE, objectManager, objectStorage, objectReaderFactory, 2, mockTime, retryScheduler);
    }

    @AfterEach
    void tearDown() {
        if (streamReaders != null) {
            // Clean up resources
            streamReaders = null;
        }
    }

    @Test
    public void testStreamReaderCreationAndReuse() throws Exception {
        TraceContext context = TraceContext.DEFAULT;

        // Initially no StreamReaders
        assertEquals(0, streamReaders.getActiveStreamReaderCount());

        // Create first StreamReader
        CompletableFuture<ReadDataBlock> readFuture1 = streamReaders.read(context, STREAM_ID_1, 0, 1, Integer.MAX_VALUE);
        ReadDataBlock result1 = readFuture1.get(5, TimeUnit.SECONDS);
        result1.getRecords().forEach(StreamRecordBatch::release);

        assertEquals(1, streamReaders.getActiveStreamReaderCount());

        // Read from same stream again - should reuse existing StreamReader
        CompletableFuture<ReadDataBlock> readFuture2 = streamReaders.read(context, STREAM_ID_1, 1, 2, Integer.MAX_VALUE);
        ReadDataBlock result2 = readFuture2.get(5, TimeUnit.SECONDS);
        result2.getRecords().forEach(StreamRecordBatch::release);

        // Should still have 1 StreamReader (reused)
        assertEquals(1, streamReaders.getActiveStreamReaderCount());
        verifyNoInteractions(retryScheduler);
    }

    /**
     * Given an arbitrary read failure, retry the original range after ten seconds while other reads can proceed.
     */
    @Test
    public void testReadRetriesOnceAfterFailure() throws Exception {
        when(objectManager.getObjects(eq(STREAM_ID_1), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("first read failed")))
            .thenReturn(CompletableFuture.completedFuture(List.of(objects.get(1L).metadata)));

        CompletableFuture<ReadDataBlock> readFuture = streamReaders.read(STREAM_ID_1, 0, 1, 100);
        Runnable retry = retryTask.get(5, TimeUnit.SECONDS);
        assertFalse(readFuture.isDone());
        assertEquals(0, streamReaders.getActiveStreamReaderCount());
        verify(objectManager).getObjects(eq(STREAM_ID_1), eq(0L), eq(1L), anyInt());

        ReadDataBlock otherRead = streamReaders.read(STREAM_ID_2, 0, 1, 100).get(5, TimeUnit.SECONDS);
        otherRead.getRecords().forEach(StreamRecordBatch::release);

        retry.run();
        ReadDataBlock result = readFuture.get(5, TimeUnit.SECONDS);
        try {
            assertEquals(1, result.getRecords().size());
            assertEquals(0, result.getRecords().get(0).getBaseOffset());
        } finally {
            result.getRecords().forEach(StreamRecordBatch::release);
        }
        verify(objectManager, times(2)).getObjects(eq(STREAM_ID_1), eq(0L), eq(1L), anyInt());
        verify(retryScheduler).schedule(any(Runnable.class), eq(10L), eq(TimeUnit.SECONDS));
    }

    /**
     * Given two failed attempts, propagate the second failure without scheduling another retry.
     */
    @Test
    public void testReadStopsAfterSecondFailure() throws Exception {
        IllegalArgumentException secondFailure = new IllegalArgumentException("second read failed");
        when(objectManager.getObjects(eq(STREAM_ID_1), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("first read failed")))
            .thenReturn(CompletableFuture.failedFuture(secondFailure));

        CompletableFuture<ReadDataBlock> readFuture = streamReaders.read(STREAM_ID_1, 0, 1, 100);
        retryTask.get(5, TimeUnit.SECONDS).run();
        ExecutionException exception = assertThrows(ExecutionException.class, () -> readFuture.get(5, TimeUnit.SECONDS));
        assertSame(secondFailure, FutureUtil.cause(exception));
        assertEquals(0, streamReaders.getActiveStreamReaderCount());
        verify(objectManager, times(2)).getObjects(eq(STREAM_ID_1), anyLong(), anyLong(), anyInt());
        verify(retryScheduler).schedule(any(Runnable.class), eq(10L), eq(TimeUnit.SECONDS));
    }

    /**
     * Given a rejected retry task, complete the read exceptionally instead of leaving it pending.
     */
    @Test
    public void testRejectedRetryCompletesRead() throws Exception {
        when(objectManager.getObjects(eq(STREAM_ID_1), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("first read failed")));
        RejectedExecutionException rejection = new RejectedExecutionException("scheduler stopped");
        doThrow(rejection).when(retryScheduler).schedule(any(Runnable.class), eq(10L), eq(TimeUnit.SECONDS));

        CompletableFuture<ReadDataBlock> readFuture = streamReaders.read(STREAM_ID_1, 0, 1, 100);
        ExecutionException exception = assertThrows(ExecutionException.class, () -> readFuture.get(5, TimeUnit.SECONDS));
        assertSame(rejection, FutureUtil.cause(exception));
        verify(objectManager).getObjects(eq(STREAM_ID_1), anyLong(), anyLong(), anyInt());
    }

    @Test
    public void testCleanupTrigger() throws Exception {
        TraceContext context = TraceContext.DEFAULT;

        // Create some StreamReaders
        CompletableFuture<ReadDataBlock> readFuture1 = streamReaders.read(context, STREAM_ID_1, 0, 1, Integer.MAX_VALUE);
        ReadDataBlock result1 = readFuture1.get(5, TimeUnit.SECONDS);
        result1.getRecords().forEach(StreamRecordBatch::release);

        CompletableFuture<ReadDataBlock> readFuture2 = streamReaders.read(context, STREAM_ID_2, 0, 1, Integer.MAX_VALUE);
        ReadDataBlock result2 = readFuture2.get(5, TimeUnit.SECONDS);
        result2.getRecords().forEach(StreamRecordBatch::release);

        assertEquals(2, streamReaders.getActiveStreamReaderCount());

        // Trigger cleanup - should not affect non-expired readers
        streamReaders.triggerExpiredStreamReaderCleanup();

        // Wait for async cleanup to complete
        await().atMost(1, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .until(() -> streamReaders.getActiveStreamReaderCount() == 2);

        // StreamReaders should still be there (not expired yet)
        assertEquals(2, streamReaders.getActiveStreamReaderCount());
    }

    @Test
    public void testExpiredStreamReaderCleanupExecution() throws Exception {
        TraceContext context = TraceContext.DEFAULT;

        // Create a StreamReader
        CompletableFuture<ReadDataBlock> readFuture = streamReaders.read(context, STREAM_ID_1, 0, 1, Integer.MAX_VALUE);
        ReadDataBlock result = readFuture.get(5, TimeUnit.SECONDS);
        result.getRecords().forEach(StreamRecordBatch::release);

        assertEquals(1, streamReaders.getActiveStreamReaderCount());

        // Advance mock time to simulate expiration (advance by 2 minutes, expiration is 1 minute)
        mockTime.sleep(TimeUnit.MINUTES.toMillis(2));

        // Trigger cleanup - should now clean up expired StreamReaders
        streamReaders.triggerExpiredStreamReaderCleanup();

        // Wait for async cleanup to complete
        await().atMost(5, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .until(() -> streamReaders.getActiveStreamReaderCount() == 0);

        // Verify system still works after cleanup
        CompletableFuture<ReadDataBlock> readFuture2 = streamReaders.read(context, STREAM_ID_2, 0, 1, Integer.MAX_VALUE);
        ReadDataBlock result2 = readFuture2.get(5, TimeUnit.SECONDS);
        result2.getRecords().forEach(StreamRecordBatch::release);

        assertEquals(1, streamReaders.getActiveStreamReaderCount());
    }



}
