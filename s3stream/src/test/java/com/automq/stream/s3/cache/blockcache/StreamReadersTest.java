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

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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

    @BeforeEach
    void setup() {
        objects = new HashMap<>();

        // Create mock objects for testing with different offset ranges
        // Object 1: STREAM_ID_1 offset 0-2
        objects.put(1L, MockObject.builder(1L, BLOCK_SIZE_THRESHOLD).write(STREAM_ID_1, List.of(
            new StreamRecordBatch(STREAM_ID_1, 0, 0, 2, TestUtils.random(100))
        )).build());
        // Object 2: STREAM_ID_2 offset 0-1
        objects.put(2L, MockObject.builder(2L, BLOCK_SIZE_THRESHOLD).write(STREAM_ID_2, List.of(
            new StreamRecordBatch(STREAM_ID_2, 0, 0, 1, TestUtils.random(100))
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

        streamReaders = new StreamReaders(Long.MAX_VALUE, objectManager, objectStorage, objectReaderFactory, 2);
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

        // Use reflection to modify lastStreamReaderExpiredCheckTime to simulate time passage
        // This forces the cleanup to consider StreamReaders as expired
        forceExpiredStreamReaderCleanup();

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

    /**
     * Force expired StreamReader cleanup by modifying lastStreamReaderExpiredCheckTime
     * and StreamReader lastAccessTimestamp using reflection to simulate time passage
     * beyond the expiration threshold.
     */
    private void forceExpiredStreamReaderCleanup() throws Exception {
        // Get the caches field from StreamReaders
        Field cachesField = StreamReaders.class.getDeclaredField("caches");
        cachesField.setAccessible(true);
        Object[] caches = (Object[]) cachesField.get(streamReaders);

        // Time far in the past to force expiration (current time - 2 minutes, expiration is 1 minute)
        long pastTime = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(2);

        // For each cache, modify lastStreamReaderExpiredCheckTime and StreamReader timestamps
        for (Object cache : caches) {
            // Modify lastStreamReaderExpiredCheckTime to force cleanup check
            Field lastCheckTimeField = cache.getClass().getDeclaredField("lastStreamReaderExpiredCheckTime");
            lastCheckTimeField.setAccessible(true);
            lastCheckTimeField.setLong(cache, pastTime);

            // Get streamReaders map and modify each StreamReader's lastAccessTimestamp
            Field streamReadersField = cache.getClass().getDeclaredField("streamReaders");
            streamReadersField.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.Map<Object, Object> streamReadersMap = (java.util.Map<Object, Object>) streamReadersField.get(cache);

            for (Object streamReader : streamReadersMap.values()) {
                Field lastAccessTimestampField = streamReader.getClass().getDeclaredField("lastAccessTimestamp");
                lastAccessTimestampField.setAccessible(true);
                lastAccessTimestampField.setLong(streamReader, pastTime);
            }
        }
    }

}
