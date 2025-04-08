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

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.CommitStreamSetObjectResponse;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.automq.stream.s3.TestUtils.random;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class DefaultUploadWriteAheadLogTaskTest {
    ObjectManager objectManager;
    ObjectStorage objectStorage;
    DefaultUploadWriteAheadLogTask deltaWALUploadTask;

    @BeforeEach
    public void setup() {
        objectManager = mock(ObjectManager.class);
        objectStorage = new MemoryObjectStorage();
    }

    @Test
    public void testUpload() throws Exception {
        AtomicLong objectIdAlloc = new AtomicLong(10);
        doAnswer(invocation -> CompletableFuture.completedFuture(objectIdAlloc.getAndIncrement())).when(objectManager).prepareObject(anyInt(), anyLong());
        when(objectManager.commitStreamSetObject(any())).thenReturn(CompletableFuture.completedFuture(new CommitStreamSetObjectResponse()));

        Map<Long, List<StreamRecordBatch>> map = new HashMap<>();
        map.put(233L, List.of(
            new StreamRecordBatch(233, 0, 10, 2, random(512)),
            new StreamRecordBatch(233, 0, 12, 2, random(128)),
            new StreamRecordBatch(233, 0, 14, 2, random(512))
        ));
        map.put(234L, List.of(
            new StreamRecordBatch(234, 0, 20, 2, random(128)),
            new StreamRecordBatch(234, 0, 22, 2, random(128))
        ));

        Config config = new Config()
            .objectBlockSize(16 * 1024 * 1024)
            .objectPartSize(16 * 1024 * 1024)
            .streamSplitSize(1000);
        deltaWALUploadTask = DefaultUploadWriteAheadLogTask.builder().config(config).streamRecordsMap(map).objectManager(objectManager)
            .objectStorage(objectStorage).executor(ForkJoinPool.commonPool()).build();

        deltaWALUploadTask.prepare().get();
        deltaWALUploadTask.upload().get();
        deltaWALUploadTask.commit().get();

        // Release all the buffers
        map.values().forEach(batches -> batches.forEach(StreamRecordBatch::release));

        ArgumentCaptor<CommitStreamSetObjectRequest> reqArg = ArgumentCaptor.forClass(CommitStreamSetObjectRequest.class);
        verify(objectManager, times(1)).commitStreamSetObject(reqArg.capture());
        // expect
        // - stream233 split
        // - stream234 write to one stream range
        CommitStreamSetObjectRequest request = reqArg.getValue();
        assertEquals(10, request.getObjectId());
        assertEquals(1, request.getStreamRanges().size());
        assertEquals(234, request.getStreamRanges().get(0).getStreamId());
        assertEquals(20, request.getStreamRanges().get(0).getStartOffset());
        assertEquals(24, request.getStreamRanges().get(0).getEndOffset());

        assertEquals(1, request.getStreamObjects().size());
        StreamObject streamObject = request.getStreamObjects().get(0);
        assertEquals(233, streamObject.getStreamId());
        assertEquals(11, streamObject.getObjectId());
        assertEquals(10, streamObject.getStartOffset());
        assertEquals(16, streamObject.getEndOffset());

        {
            S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(request.getObjectId(), request.getObjectSize(), S3ObjectType.STREAM_SET);
            ObjectReader objectReader = ObjectReader.reader(s3ObjectMetadata, objectStorage);
            DataBlockIndex blockIndex = objectReader.find(234, 20, 24).get()
                .streamDataBlocks().get(0).dataBlockIndex();
            ObjectReader.DataBlockGroup dataBlockGroup = objectReader.read(blockIndex).get();
            try (CloseableIterator<StreamRecordBatch> it = dataBlockGroup.iterator()) {
                StreamRecordBatch record = it.next();
                assertEquals(20, record.getBaseOffset());
                record = it.next();
                assertEquals(24, record.getLastOffset());
                record.release();
            }
        }

        {
            S3ObjectMetadata streamObjectMetadata = new S3ObjectMetadata(11, request.getStreamObjects().get(0).getObjectSize(), S3ObjectType.STREAM);
            ObjectReader objectReader = ObjectReader.reader(streamObjectMetadata, objectStorage);
            DataBlockIndex blockIndex = objectReader.find(233, 10, 16).get()
                .streamDataBlocks().get(0).dataBlockIndex();
            ObjectReader.DataBlockGroup dataBlockGroup = objectReader.read(blockIndex).get();
            try (CloseableIterator<StreamRecordBatch> it = dataBlockGroup.iterator()) {
                StreamRecordBatch r1 = it.next();
                assertEquals(10, r1.getBaseOffset());
                r1.release();
                StreamRecordBatch r2 = it.next();
                assertEquals(12, r2.getBaseOffset());
                r2.release();
                StreamRecordBatch r3 = it.next();
                assertEquals(14, r3.getBaseOffset());
                r3.release();
            }
        }
    }

    @Test
    public void testUpload_oneStream() throws Exception {
        AtomicLong objectIdAlloc = new AtomicLong(10);
        doAnswer(invocation -> CompletableFuture.completedFuture(objectIdAlloc.getAndIncrement())).when(objectManager).prepareObject(anyInt(), anyLong());
        when(objectManager.commitStreamSetObject(any())).thenReturn(CompletableFuture.completedFuture(new CommitStreamSetObjectResponse()));

        Map<Long, List<StreamRecordBatch>> map = new HashMap<>();
        map.put(233L, List.of(
            new StreamRecordBatch(233, 0, 10, 2, random(512)),
            new StreamRecordBatch(233, 0, 12, 2, random(128)),
            new StreamRecordBatch(233, 0, 14, 2, random(512))
        ));
        Config config = new Config()
            .objectBlockSize(16 * 1024 * 1024)
            .objectPartSize(16 * 1024 * 1024)
            .streamSplitSize(16 * 1024 * 1024);
        deltaWALUploadTask = DefaultUploadWriteAheadLogTask.builder().config(config).streamRecordsMap(map).objectManager(objectManager)
            .objectStorage(objectStorage).executor(ForkJoinPool.commonPool()).build();

        deltaWALUploadTask.prepare().get();
        deltaWALUploadTask.upload().get();
        deltaWALUploadTask.commit().get();

        // Release all the buffers
        map.values().forEach(batches -> batches.forEach(StreamRecordBatch::release));

        ArgumentCaptor<CommitStreamSetObjectRequest> reqArg = ArgumentCaptor.forClass(CommitStreamSetObjectRequest.class);
        verify(objectManager, times(1)).commitStreamSetObject(reqArg.capture());
        CommitStreamSetObjectRequest request = reqArg.getValue();
        assertEquals(0, request.getObjectSize());
        assertEquals(0, request.getStreamRanges().size());
        assertEquals(1, request.getStreamObjects().size());
    }

    @Test
    public void test_emptyWALData() throws ExecutionException, InterruptedException, TimeoutException {
        AtomicLong objectIdAlloc = new AtomicLong(10);
        doAnswer(invocation -> CompletableFuture.completedFuture(objectIdAlloc.getAndIncrement())).when(objectManager).prepareObject(anyInt(), anyLong());
        when(objectManager.commitStreamSetObject(any())).thenReturn(CompletableFuture.completedFuture(new CommitStreamSetObjectResponse()));

        Map<Long, List<StreamRecordBatch>> map = new HashMap<>();
        map.put(233L, List.of(
            new StreamRecordBatch(233, 0, 10, 2, random(512))
        ));
        map.put(234L, List.of(
            new StreamRecordBatch(234, 0, 20, 2, random(128))
        ));

        Config config = new Config()
            .objectBlockSize(16 * 1024 * 1024)
            .objectPartSize(16 * 1024 * 1024)
            .streamSplitSize(64);
        deltaWALUploadTask = DefaultUploadWriteAheadLogTask.builder().config(config).streamRecordsMap(map).objectManager(objectManager)
            .objectStorage(objectStorage).executor(ForkJoinPool.commonPool()).build();
        assertTrue(deltaWALUploadTask.forceSplit);
    }
}
