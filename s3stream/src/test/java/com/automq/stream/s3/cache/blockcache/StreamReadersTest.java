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

        // Create mock objects for testing
        objects.put(1L, MockObject.builder(1L, BLOCK_SIZE_THRESHOLD).write(STREAM_ID_1, List.of(
            new StreamRecordBatch(STREAM_ID_1, 0, 0, 1, TestUtils.random(100))
        )).build());

        objects.put(2L, MockObject.builder(2L, BLOCK_SIZE_THRESHOLD).write(STREAM_ID_2, List.of(
            new StreamRecordBatch(STREAM_ID_2, 0, 0, 1, TestUtils.random(100))
        )).build());

        objectManager = mock(ObjectManager.class);
        when(objectManager.isObjectExist(anyLong())).thenReturn(true);
        // Mock getObjects method to return our test objects
        when(objectManager.getObjects(eq(STREAM_ID_1), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(List.of(objects.get(1L).metadata)));
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

        // Read from different stream - should create new StreamReader
        CompletableFuture<ReadDataBlock> readFuture3 = streamReaders.read(context, STREAM_ID_2, 0, 1, Integer.MAX_VALUE);
        ReadDataBlock result3 = readFuture3.get(5, TimeUnit.SECONDS);
        result3.getRecords().forEach(StreamRecordBatch::release);

        assertEquals(2, streamReaders.getActiveStreamReaderCount());
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

        // Wait a bit for async cleanup to complete
        Thread.sleep(200);

        // StreamReaders should still be there (not expired yet)
        assertEquals(2, streamReaders.getActiveStreamReaderCount());
    }

    @Test
    public void testPeriodicCleanupExecution() throws Exception {
        TraceContext context = TraceContext.DEFAULT;

        // Create a StreamReader
        CompletableFuture<ReadDataBlock> readFuture = streamReaders.read(context, STREAM_ID_1, 0, 1, Integer.MAX_VALUE);
        ReadDataBlock result = readFuture.get(5, TimeUnit.SECONDS);
        result.getRecords().forEach(StreamRecordBatch::release);

        assertEquals(1, streamReaders.getActiveStreamReaderCount());

        // Wait for the periodic cleanup task to execute
        // The scheduled task runs every 1 minutes (STREAM_READER_EXPIRED_CHECK_INTERVAL_MILLS)
        // After 1+ minutes, the StreamReader will be expired and should be cleaned up
        await().atMost(150, TimeUnit.SECONDS)  // Wait up to 2.5 minutes （1m delay + 1m interval + 0.5m buffer）
               .pollInterval(10, TimeUnit.SECONDS)  // Check every 10 seconds
               .until(() -> {
                   // The periodic task should eventually clean up the expired StreamReader
                   return streamReaders.getActiveStreamReaderCount() == 0;
               });

        // Verify system still works after cleanup
        CompletableFuture<ReadDataBlock> readFuture2 = streamReaders.read(context, STREAM_ID_2, 0, 1, Integer.MAX_VALUE);
        ReadDataBlock result2 = readFuture2.get(5, TimeUnit.SECONDS);
        result2.getRecords().forEach(StreamRecordBatch::release);

        assertEquals(1, streamReaders.getActiveStreamReaderCount());
    }

}
