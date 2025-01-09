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

package com.automq.stream.s3.compact;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectWriter;
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.compact.operator.DataBlockReader;
import com.automq.stream.s3.compact.utils.CompactionUtils;
import com.automq.stream.s3.memory.MemoryMetadataManager;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.AwsObjectStorage;
import com.automq.stream.s3.operator.MemoryObjectStorage;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@Timeout(60)
@Tag("S3Unit")
public class CompactionManagerTest extends CompactionTestBase {
    private static final int BROKER0 = 0;
    private CompactionAnalyzer compactionAnalyzer;
    private CompactionManager compactionManager;
    private Config config;

    private static Map<Long, List<StreamDataBlock>> getStreamDataBlockMap() {
        StreamDataBlock block1 = new StreamDataBlock(OBJECT_0, new DataBlockIndex(0, 0, 15, 15, 0, 15));
        StreamDataBlock block2 = new StreamDataBlock(OBJECT_0, new DataBlockIndex(1, 0, 20, 20, 15, 50));

        StreamDataBlock block3 = new StreamDataBlock(OBJECT_1, new DataBlockIndex(0, 15, 12, 12, 0, 20));
        StreamDataBlock block4 = new StreamDataBlock(OBJECT_1, new DataBlockIndex(1, 20, 25, 25, 20, 60));

        StreamDataBlock block5 = new StreamDataBlock(OBJECT_2, new DataBlockIndex(0, 27, 13, 20, 0, 20));
        StreamDataBlock block6 = new StreamDataBlock(OBJECT_2, new DataBlockIndex(3, 0, 30, 30, 20, 30));
        return Map.of(
            OBJECT_0, List.of(
                block1,
                block2
            ),
            OBJECT_1, List.of(
                block3,
                block4
            ),
            OBJECT_2, List.of(
                block5,
                block6
            )
        );
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        config = Mockito.mock(Config.class);
        when(config.nodeId()).thenReturn(BROKER0);
        when(config.streamSetObjectCompactionUploadConcurrency()).thenReturn(3);
        when(config.objectPartSize()).thenReturn(100);
        when(config.streamSetObjectCompactionCacheSize()).thenReturn(300L);
        when(config.streamSetObjectCompactionStreamSplitSize()).thenReturn(100L);
        when(config.streamSetObjectCompactionForceSplitPeriod()).thenReturn(120);
        when(config.streamSetObjectCompactionMaxObjectNum()).thenReturn(100);
        when(config.maxStreamNumPerStreamSetObject()).thenReturn(100);
        when(config.maxStreamObjectNumPerCommit()).thenReturn(100);
//        when(config.networkInboundBaselineBandwidth()).thenReturn(1000L);
        compactionAnalyzer = new CompactionAnalyzer(config.streamSetObjectCompactionCacheSize(), config.streamSetObjectCompactionStreamSplitSize(),
            config.maxStreamNumPerStreamSetObject(), config.maxStreamObjectNumPerCommit());
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();
        if (compactionManager != null) {
            compactionManager.shutdown();
        }
    }

    @Test
    public void testForceSplit() {
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        when(config.streamSetObjectCompactionForceSplitPeriod()).thenReturn(0);
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);

        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(0)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(0));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_0), request.getCompactedObjectIds());
        Assertions.assertEquals(3, request.getStreamObjects().size());
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, Collections.singletonList(s3ObjectMetadata.get(0)), request));

        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(1)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(1));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_1), request.getCompactedObjectIds());
        Assertions.assertEquals(2, request.getStreamObjects().size());
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, Collections.singletonList(s3ObjectMetadata.get(1)), request));

        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(2)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(2));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_2), request.getCompactedObjectIds());
        Assertions.assertEquals(2, request.getStreamObjects().size());
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, Collections.singletonList(s3ObjectMetadata.get(2)), request));
    }

    @Test
    public void testForceSplitWithOutDatedObject() {
        when(streamManager.getStreams(Collections.emptyList())).thenReturn(CompletableFuture.completedFuture(
            List.of(new StreamMetadata(STREAM_0, 0, 999, 9999, StreamState.OPENED),
                new StreamMetadata(STREAM_1, 0, 999, 9999, StreamState.OPENED),
                new StreamMetadata(STREAM_2, 0, 999, 9999, StreamState.OPENED))));

        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        when(config.streamSetObjectCompactionForceSplitPeriod()).thenReturn(0);
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);

        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(0)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(0));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_0), request.getCompactedObjectIds());
        Assertions.assertTrue(request.getStreamObjects().isEmpty());
        Assertions.assertTrue(request.getStreamRanges().isEmpty());

        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(1)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(1));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_1), request.getCompactedObjectIds());
        Assertions.assertTrue(request.getStreamObjects().isEmpty());
        Assertions.assertTrue(request.getStreamRanges().isEmpty());

        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(2)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(2));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_2), request.getCompactedObjectIds());
        Assertions.assertTrue(request.getStreamObjects().isEmpty());
        Assertions.assertTrue(request.getStreamRanges().isEmpty());
    }


    @Test
    public void testForceSplitWithNonExistStream() {
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        streamMetadataList = streamMetadataList.stream().filter(s -> s.streamId() != STREAM_0).collect(Collectors.toList());
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        when(config.streamSetObjectCompactionForceSplitPeriod()).thenReturn(0);
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);

        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(0)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(0));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_0), request.getCompactedObjectIds());
        Assertions.assertEquals(2, request.getStreamObjects().size());
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, Collections.singletonList(s3ObjectMetadata.get(0)), request));

        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(1)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(1));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_1), request.getCompactedObjectIds());
        Assertions.assertEquals(1, request.getStreamObjects().size());
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, Collections.singletonList(s3ObjectMetadata.get(1)), request));

        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(2)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(2));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_2), request.getCompactedObjectIds());
        Assertions.assertEquals(2, request.getStreamObjects().size());
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, Collections.singletonList(s3ObjectMetadata.get(2)), request));
    }

    @Test
    public void testForceSplitWithEmptyStreamList() {
        List<StreamMetadata> streamMetadataList = Collections.emptyList();
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        when(config.streamSetObjectCompactionForceSplitPeriod()).thenReturn(0);
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);

        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(0)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(0));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_0), request.getCompactedObjectIds());
        Assertions.assertTrue(request.getStreamObjects().isEmpty());
        Assertions.assertTrue(request.getStreamRanges().isEmpty());

        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(1)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(1));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_1), request.getCompactedObjectIds());
        Assertions.assertTrue(request.getStreamObjects().isEmpty());
        Assertions.assertTrue(request.getStreamRanges().isEmpty());

        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(2)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(2));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_2), request.getCompactedObjectIds());
        Assertions.assertTrue(request.getStreamObjects().isEmpty());
        Assertions.assertTrue(request.getStreamRanges().isEmpty());
    }

    @Test
    public void testForceSplitWithException() {
        S3AsyncClient s3AsyncClient = Mockito.mock(S3AsyncClient.class);
        doAnswer(invocation -> CompletableFuture.completedFuture(null)).when(s3AsyncClient).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));

        Map<Long, List<StreamDataBlock>> streamDataBlockMap = getStreamDataBlockMap();
        S3ObjectMetadata objectMetadata = new S3ObjectMetadata(OBJECT_0, 0, S3ObjectType.STREAM_SET);
        AwsObjectStorage objectStorage = Mockito.spy(new AwsObjectStorage(s3AsyncClient, ""));
        doReturn(CompletableFuture.failedFuture(new IllegalArgumentException("exception"))).when(objectStorage).rangeRead(any(), eq(objectMetadata.key()), anyLong(), anyLong());

        CompactionManager compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        Assertions.assertThrowsExactly(CompletionException.class, () -> compactionManager.groupAndSplitStreamDataBlocks(objectMetadata, streamDataBlockMap.get(OBJECT_0)));
    }

    @Test
    public void testForceSplitWithLimit() {
        when(config.streamSetObjectCompactionCacheSize()).thenReturn(5L);
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        when(config.streamSetObjectCompactionForceSplitPeriod()).thenReturn(0);
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);

        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(0)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(0));
        Assertions.assertNull(request);
    }

    @Test
    public void testForceSplitWithLimit2() {
        when(config.streamSetObjectCompactionCacheSize()).thenReturn(150L);
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        when(config.streamSetObjectCompactionForceSplitPeriod()).thenReturn(0);
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);

        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        compactionManager.updateStreamDataBlockMap(List.of(s3ObjectMetadata.get(0)));
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(0));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_0), request.getCompactedObjectIds());
        Assertions.assertEquals(3, request.getStreamObjects().size());
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, Collections.singletonList(s3ObjectMetadata.get(0)), request));
    }

    @Test
    public void testCompact() {
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        compactionManager.updateStreamDataBlockMap(s3ObjectMetadata);
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, s3ObjectMetadata);

        assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_2));
        assertEquals(3, request.getStreamObjects().size());
        assertEquals(2, request.getStreamRanges().size());

        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, s3ObjectMetadata, request));
    }

    @Test
    public void testCompactSingleObject() {
        List<S3ObjectMetadata> s3ObjectMetadataList = new ArrayList<>();
        objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30)).thenAccept(objectId -> {
            assertEquals(OBJECT_3, objectId);
            ObjectWriter objectWriter = ObjectWriter.writer(OBJECT_3, objectStorage, 1024, 1024);
            StreamRecordBatch r1 = new StreamRecordBatch(STREAM_1, 0, 500, 20, TestUtils.random(20));
            StreamRecordBatch r2 = new StreamRecordBatch(STREAM_3, 0, 0, 10, TestUtils.random(1024));
            StreamRecordBatch r3 = new StreamRecordBatch(STREAM_3, 0, 10, 10, TestUtils.random(1024));
            objectWriter.write(STREAM_1, List.of(r1));
            objectWriter.write(STREAM_3, List.of(r2, r3));
            objectWriter.close().join();
            List<StreamOffsetRange> streamsIndices = List.of(
                new StreamOffsetRange(STREAM_1, 500, 520),
                new StreamOffsetRange(STREAM_3, 0, 20)
            );
            S3ObjectMetadata objectMetadata = new S3ObjectMetadata(OBJECT_3, S3ObjectType.STREAM_SET, streamsIndices, System.currentTimeMillis(),
                System.currentTimeMillis(), objectWriter.size(), OBJECT_3);
            s3ObjectMetadataList.add(objectMetadata);
            List.of(r1, r2, r3).forEach(StreamRecordBatch::release);
        }).join();
        when(streamManager.getStreams(Collections.emptyList())).thenReturn(CompletableFuture.completedFuture(
            List.of(new StreamMetadata(STREAM_0, 0, 1024, 2048, StreamState.OPENED),
                new StreamMetadata(STREAM_3, 0, 1024, 2048, StreamState.OPENED))));
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        compactionManager.updateStreamDataBlockMap(s3ObjectMetadataList);
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, s3ObjectMetadataList);
        assertEquals(-1L, request.getObjectId());
        assertEquals(List.of(OBJECT_3), request.getCompactedObjectIds());
        assertTrue(request.getStreamObjects().isEmpty());
        assertTrue(request.getStreamRanges().isEmpty());

        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, s3ObjectMetadataList, request));
    }

    @Test
    public void testCompactWithDataTrimmed() {
        when(streamManager.getStreams(Collections.emptyList())).thenReturn(CompletableFuture.completedFuture(
            List.of(new StreamMetadata(STREAM_0, 0, 5, 20, StreamState.OPENED),
                new StreamMetadata(STREAM_1, 0, 25, 500, StreamState.OPENED),
                new StreamMetadata(STREAM_2, 0, 30, 270, StreamState.OPENED))));
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        compactionManager.updateStreamDataBlockMap(S3_WAL_OBJECT_METADATA_LIST);
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST);

        assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_2));
        assertEquals(3, request.getStreamObjects().size());
        assertEquals(2, request.getStreamRanges().size());

        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, request));
    }

    @Test
    public void testCompactWithDataTrimmed2() {
        when(streamManager.getStreams(Collections.emptyList())).thenReturn(CompletableFuture.completedFuture(
            List.of(new StreamMetadata(STREAM_0, 0, 15, 20, StreamState.OPENED),
                new StreamMetadata(STREAM_1, 0, 25, 500, StreamState.OPENED),
                new StreamMetadata(STREAM_2, 0, 30, 270, StreamState.OPENED))));
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        compactionManager.updateStreamDataBlockMap(S3_WAL_OBJECT_METADATA_LIST);
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST);

        assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_2));
        assertEquals(3, request.getStreamObjects().size());
        assertEquals(2, request.getStreamRanges().size());

        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, request));
    }

    @Test
    public void testCompactionWithDataTrimmed3() {
        objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30)).thenAccept(objectId -> {
            assertEquals(OBJECT_3, objectId);
            ObjectWriter objectWriter = ObjectWriter.writer(OBJECT_3, objectStorage, 1024, 1024);
            StreamRecordBatch r1 = new StreamRecordBatch(STREAM_1, 0, 500, 20, TestUtils.random(20));
            StreamRecordBatch r2 = new StreamRecordBatch(STREAM_3, 0, 0, 10, TestUtils.random(1024));
            StreamRecordBatch r3 = new StreamRecordBatch(STREAM_3, 0, 10, 10, TestUtils.random(1024));
            objectWriter.write(STREAM_1, List.of(r1));
            objectWriter.write(STREAM_3, List.of(r2, r3));
            objectWriter.close().join();
            List<StreamOffsetRange> streamsIndices = List.of(
                new StreamOffsetRange(STREAM_1, 500, 520),
                new StreamOffsetRange(STREAM_3, 0, 20)
            );
            S3ObjectMetadata objectMetadata = new S3ObjectMetadata(OBJECT_3, S3ObjectType.STREAM_SET, streamsIndices, System.currentTimeMillis(),
                System.currentTimeMillis(), objectWriter.size(), OBJECT_3);
            S3_WAL_OBJECT_METADATA_LIST.add(objectMetadata);
            List.of(r1, r2, r3).forEach(StreamRecordBatch::release);
        }).join();
        when(streamManager.getStreams(Collections.emptyList())).thenReturn(CompletableFuture.completedFuture(
            List.of(new StreamMetadata(STREAM_0, 0, 0, 20, StreamState.OPENED),
                new StreamMetadata(STREAM_1, 0, 25, 500, StreamState.OPENED),
                new StreamMetadata(STREAM_2, 0, 30, 270, StreamState.OPENED),
                new StreamMetadata(STREAM_3, 0, 10, 20, StreamState.OPENED))));
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        compactionManager.updateStreamDataBlockMap(S3_WAL_OBJECT_METADATA_LIST);
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        Assertions.assertThrows(IllegalStateException.class, () -> compactionManager.collectStreamIdsAndCheckBlockSize());
    }

    @Test
    public void testCompactionWithDataTrimmed4() {
        objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30)).thenAccept(objectId -> {
            assertEquals(OBJECT_3, objectId);
            ObjectWriter objectWriter = ObjectWriter.writer(OBJECT_3, objectStorage, 200, 1024);
            StreamRecordBatch r1 = new StreamRecordBatch(STREAM_1, 0, 500, 20, TestUtils.random(20));
            StreamRecordBatch r2 = new StreamRecordBatch(STREAM_3, 0, 0, 10, TestUtils.random(200));
            StreamRecordBatch r3 = new StreamRecordBatch(STREAM_3, 0, 10, 10, TestUtils.random(200));
            objectWriter.write(STREAM_1, List.of(r1));
            objectWriter.write(STREAM_3, List.of(r2, r3));
            objectWriter.close().join();
            List<StreamOffsetRange> streamsIndices = List.of(
                new StreamOffsetRange(STREAM_1, 500, 520),
                new StreamOffsetRange(STREAM_3, 0, 20)
            );
            S3ObjectMetadata objectMetadata = new S3ObjectMetadata(OBJECT_3, S3ObjectType.STREAM_SET, streamsIndices, System.currentTimeMillis(),
                System.currentTimeMillis(), objectWriter.size(), OBJECT_3);
            S3_WAL_OBJECT_METADATA_LIST.add(objectMetadata);
            List.of(r1, r2, r3).forEach(StreamRecordBatch::release);
        }).join();
        when(streamManager.getStreams(Collections.emptyList())).thenReturn(CompletableFuture.completedFuture(
            List.of(new StreamMetadata(STREAM_0, 0, 0, 20, StreamState.OPENED),
                new StreamMetadata(STREAM_1, 0, 25, 500, StreamState.OPENED),
                new StreamMetadata(STREAM_2, 0, 30, 270, StreamState.OPENED),
                new StreamMetadata(STREAM_3, 0, 10, 20, StreamState.OPENED))));
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        compactionManager.updateStreamDataBlockMap(S3_WAL_OBJECT_METADATA_LIST);
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST);

        assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2, OBJECT_3), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_3);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_3));
        assertEquals(4, request.getStreamObjects().size());
        assertEquals(2, request.getStreamRanges().size());

        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, request));
    }

    @Test
    public void testCompactWithOutdatedObject() {
        when(streamManager.getStreams(Collections.emptyList())).thenReturn(CompletableFuture.completedFuture(
            List.of(new StreamMetadata(STREAM_0, 0, 15, 20, StreamState.OPENED),
                new StreamMetadata(STREAM_1, 0, 60, 500, StreamState.OPENED),
                new StreamMetadata(STREAM_2, 0, 60, 270, StreamState.OPENED))));
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        compactionManager.updateStreamDataBlockMap(S3_WAL_OBJECT_METADATA_LIST);
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST);

        assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_2));
        assertEquals(2, request.getStreamObjects().size());
        assertEquals(2, request.getStreamRanges().size());

        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, request));
    }

    @Test
    public void testCompactWithNonExistStream() {
        when(streamManager.getStreams(Collections.emptyList())).thenReturn(CompletableFuture.completedFuture(
            List.of(new StreamMetadata(STREAM_1, 0, 25, 500, StreamState.OPENED),
                new StreamMetadata(STREAM_2, 0, 30, 270, StreamState.OPENED))));
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        compactionManager.updateStreamDataBlockMap(S3_WAL_OBJECT_METADATA_LIST);
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST);

        Set<Long> streamIds = request.getStreamObjects().stream().map(StreamObject::getStreamId).collect(Collectors.toSet());
        streamIds.addAll(request.getStreamRanges().stream().map(ObjectStreamRange::getStreamId).collect(Collectors.toSet()));
        assertEquals(Set.of(STREAM_1, STREAM_2), streamIds);
        assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_2));
        assertEquals(3, request.getStreamObjects().size());
        assertEquals(1, request.getStreamRanges().size());

        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, request));
    }

    @Test
    public void testCompactWithEmptyStream() {
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        List<StreamMetadata> streamMetadataList = Collections.emptyList();
        compactionManager.updateStreamDataBlockMap(S3_WAL_OBJECT_METADATA_LIST);
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST);

        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertTrue(request.getStreamObjects().isEmpty());
        Assertions.assertTrue(request.getStreamRanges().isEmpty());
        assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, request));
    }

    @Test
    public void testCompactNoneExistObjects() {
        when(config.streamSetObjectCompactionStreamSplitSize()).thenReturn(100L);
        when(config.streamSetObjectCompactionCacheSize()).thenReturn(9999L);
        Map<Long, List<StreamDataBlock>> streamDataBlockMap = getStreamDataBlockMap();
        S3ObjectMetadata objectMetadata0 = new S3ObjectMetadata(OBJECT_0, 0, S3ObjectType.STREAM_SET);
        S3ObjectMetadata objectMetadata1 = new S3ObjectMetadata(OBJECT_1, 0, S3ObjectType.STREAM_SET);
        S3ObjectMetadata objectMetadata2 = new S3ObjectMetadata(OBJECT_2, 0, S3ObjectType.STREAM_SET);
        List<S3ObjectMetadata> s3ObjectMetadata = List.of(objectMetadata0, objectMetadata1, objectMetadata2);
        this.compactionAnalyzer = new CompactionAnalyzer(config.streamSetObjectCompactionCacheSize(), config.streamSetObjectCompactionStreamSplitSize(),
            config.maxStreamNumPerStreamSetObject(), config.maxStreamObjectNumPerCommit());
        List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(streamDataBlockMap, new HashSet<>());
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();

        S3AsyncClient s3AsyncClient = Mockito.mock(S3AsyncClient.class);
        doAnswer(invocation -> CompletableFuture.completedFuture(null)).when(s3AsyncClient).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));

        AwsObjectStorage objectStorage = Mockito.spy(new AwsObjectStorage(s3AsyncClient, ""));
        doAnswer(invocation -> CompletableFuture.completedFuture(TestUtils.randomPooled(65))).when(objectStorage).rangeRead(any(), eq(objectMetadata0.key()), anyLong(), anyLong());
        doAnswer(invocation -> CompletableFuture.completedFuture(TestUtils.randomPooled(80))).when(objectStorage).rangeRead(any(), eq(objectMetadata1.key()), anyLong(), anyLong());
        doAnswer(invocation -> CompletableFuture.failedFuture(new IllegalArgumentException("exception"))).when(objectStorage).rangeRead(any(), eq(objectMetadata2.key()), anyLong(), anyLong());

        CompactionManager compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        Assertions.assertThrowsExactly(CompletionException.class,
            () -> compactionManager.executeCompactionPlans(request, compactionPlans, s3ObjectMetadata));
        for (CompactionPlan plan : compactionPlans) {
            plan.streamDataBlocksMap().forEach((streamId, blocks) -> blocks.forEach(block -> {
                if (block.getObjectId() != OBJECT_2) {
                    block.getDataCf().thenAccept(data -> {
                        Assertions.assertEquals(0, data.refCnt());
                    }).join();
                }
            }));
        }
    }

    @Test
    public void testCompactNoneExistObjects2() {
        when(config.streamSetObjectCompactionStreamSplitSize()).thenReturn(100L);
        when(config.streamSetObjectCompactionCacheSize()).thenReturn(9999L);
        Map<Long, List<StreamDataBlock>> streamDataBlockMap = getStreamDataBlockMap();
        S3ObjectMetadata objectMetadata0 = new S3ObjectMetadata(OBJECT_0, 0, S3ObjectType.STREAM_SET);
        S3ObjectMetadata objectMetadata1 = new S3ObjectMetadata(OBJECT_1, 0, S3ObjectType.STREAM_SET);
        S3ObjectMetadata objectMetadata2 = new S3ObjectMetadata(OBJECT_2, 0, S3ObjectType.STREAM_SET);
        List<S3ObjectMetadata> s3ObjectMetadata = List.of(objectMetadata0, objectMetadata1, objectMetadata2);
        this.compactionAnalyzer = new CompactionAnalyzer(config.streamSetObjectCompactionCacheSize(), config.streamSetObjectCompactionStreamSplitSize(),
            config.maxStreamNumPerStreamSetObject(), config.maxStreamObjectNumPerCommit());
        List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(streamDataBlockMap, new HashSet<>());
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();

        S3AsyncClient s3AsyncClient = Mockito.mock(S3AsyncClient.class);
        doAnswer(invocation -> CompletableFuture.completedFuture(null)).when(s3AsyncClient).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));

        AwsObjectStorage objectStorage = Mockito.spy(new AwsObjectStorage(s3AsyncClient, ""));
        doAnswer(invocation -> CompletableFuture.completedFuture(TestUtils.randomPooled(65))).when(objectStorage).rangeRead(any(), eq(objectMetadata0.key()), anyLong(), anyLong());
        doAnswer(invocation -> CompletableFuture.failedFuture(new IllegalArgumentException("exception"))).when(objectStorage).rangeRead(any(), eq(objectMetadata1.key()), anyLong(), anyLong());
        doAnswer(invocation -> CompletableFuture.completedFuture(TestUtils.randomPooled(50))).when(objectStorage).rangeRead(any(), eq(objectMetadata2.key()), anyLong(), anyLong());

        CompactionManager compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        Assertions.assertThrowsExactly(CompletionException.class,
            () -> compactionManager.executeCompactionPlans(request, compactionPlans, s3ObjectMetadata));
        for (CompactionPlan plan : compactionPlans) {
            plan.streamDataBlocksMap().forEach((streamId, blocks) -> blocks.forEach(block -> {
                if (block.getObjectId() != OBJECT_1) {
                    block.getDataCf().thenAccept(data -> {
                        Assertions.assertEquals(0, data.refCnt());
                    }).join();
                }
            }));
        }
    }

    @Test
    public void testCompactWithUploadException() {
        when(config.streamSetObjectCompactionStreamSplitSize()).thenReturn(100 * 1024 * 1024L);
        when(config.streamSetObjectCompactionCacheSize()).thenReturn(1024 * 1024 * 1024L);
        when(config.objectPartSize()).thenReturn(100 * 1024 * 1024);
        Map<Long, List<StreamDataBlock>> streamDataBlockMap = getStreamDataBlockMapLarge();
        S3ObjectMetadata objectMetadata0 = new S3ObjectMetadata(OBJECT_0, 0, S3ObjectType.STREAM_SET);
        S3ObjectMetadata objectMetadata1 = new S3ObjectMetadata(OBJECT_1, 0, S3ObjectType.STREAM_SET);
        S3ObjectMetadata objectMetadata2 = new S3ObjectMetadata(OBJECT_2, 0, S3ObjectType.STREAM_SET);
        List<S3ObjectMetadata> s3ObjectMetadata = List.of(objectMetadata0, objectMetadata1, objectMetadata2);
        this.compactionAnalyzer = new CompactionAnalyzer(config.streamSetObjectCompactionCacheSize(), config.streamSetObjectCompactionStreamSplitSize(),
            config.maxStreamNumPerStreamSetObject(), config.maxStreamObjectNumPerCommit());
        List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(streamDataBlockMap, new HashSet<>());
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();

        S3AsyncClient s3AsyncClient = Mockito.mock(S3AsyncClient.class);
        doAnswer(invocation -> CompletableFuture.failedFuture(S3Exception.builder().statusCode(HttpStatusCode.NOT_FOUND).build())).when(s3AsyncClient).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
        doAnswer(invocation -> CompletableFuture.completedFuture(CreateMultipartUploadResponse.builder().uploadId("123").build())).when(s3AsyncClient).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        doAnswer(invocation -> CompletableFuture.failedFuture(S3Exception.builder().statusCode(HttpStatusCode.NOT_FOUND).build())).when(s3AsyncClient).uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));

        AwsObjectStorage objectStorage = Mockito.spy(new AwsObjectStorage(s3AsyncClient, ""));
        doAnswer(invocation -> CompletableFuture.completedFuture(TestUtils.randomPooled(65 * 1024 * 1024))).when(objectStorage).rangeRead(any(), eq(objectMetadata0.key()), anyLong(), anyLong());
        doAnswer(invocation -> CompletableFuture.completedFuture(TestUtils.randomPooled(80 * 1024 * 1024))).when(objectStorage).rangeRead(any(), eq(objectMetadata1.key()), anyLong(), anyLong());
        doAnswer(invocation -> CompletableFuture.completedFuture(TestUtils.randomPooled(50 * 1024 * 1024))).when(objectStorage).rangeRead(any(), eq(objectMetadata2.key()), anyLong(), anyLong());

        CompactionManager compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        Assertions.assertThrowsExactly(CompletionException.class,
            () -> compactionManager.executeCompactionPlans(request, compactionPlans, s3ObjectMetadata));
        for (CompactionPlan plan : compactionPlans) {
            plan.streamDataBlocksMap().forEach((streamId, blocks) -> blocks.forEach(block -> {
                if (block.getObjectId() != OBJECT_1) {
                    block.getDataCf().thenAccept(data -> {
                        Assertions.assertEquals(0, data.refCnt());
                    }).join();
                }
            }));
        }
    }

    private static Map<Long, List<StreamDataBlock>> getStreamDataBlockMapLarge() {
        StreamDataBlock block1 = new StreamDataBlock(OBJECT_0, new DataBlockIndex(0, 0, 15, 15, 0, 15 * 1024 * 1024));
        StreamDataBlock block2 = new StreamDataBlock(OBJECT_0, new DataBlockIndex(1, 0, 20, 20, 15, 50 * 1024 * 1024));

        StreamDataBlock block3 = new StreamDataBlock(OBJECT_1, new DataBlockIndex(0, 15, 12, 12, 0, 20 * 1024 * 1024));
        StreamDataBlock block4 = new StreamDataBlock(OBJECT_1, new DataBlockIndex(1, 20, 25, 25, 20, 60 * 1024 * 1024));

        StreamDataBlock block5 = new StreamDataBlock(OBJECT_2, new DataBlockIndex(0, 27, 13, 20, 0, 20 * 1024 * 1024));
        StreamDataBlock block6 = new StreamDataBlock(OBJECT_2, new DataBlockIndex(3, 0, 30, 30, 20, 30 * 1024 * 1024));
        return Map.of(
            OBJECT_0, List.of(
                block1,
                block2
            ),
            OBJECT_1, List.of(
                block3,
                block4
            ),
            OBJECT_2, List.of(
                block5,
                block6
            )
        );
    }

    @Test
    public void testCompactWithLimit() {
        when(config.streamSetObjectCompactionStreamSplitSize()).thenReturn(70L);
        when(config.maxStreamNumPerStreamSetObject()).thenReturn(MAX_STREAM_NUM_IN_WAL);
        when(config.maxStreamObjectNumPerCommit()).thenReturn(4);
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        compactionManager.updateStreamDataBlockMap(s3ObjectMetadata);
        compactionManager.filterInvalidStreamDataBlocks(streamMetadataList);
        CommitStreamSetObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, s3ObjectMetadata);

        assertEquals(List.of(OBJECT_0, OBJECT_1), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_2));
        assertEquals(2, request.getStreamObjects().size());
        assertEquals(1, request.getStreamRanges().size());

        Set<Long> compactedObjectIds = new HashSet<>(request.getCompactedObjectIds());
        s3ObjectMetadata = s3ObjectMetadata.stream().filter(s -> compactedObjectIds.contains(s.objectId())).collect(Collectors.toList());
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, s3ObjectMetadata, request));
    }

    @Test
    @SuppressWarnings("unchecked")
    @Disabled
    public void testCompactionShutdown() throws Throwable {
        streamManager = Mockito.mock(MemoryMetadataManager.class);
        when(streamManager.getStreams(Mockito.anyList())).thenReturn(CompletableFuture.completedFuture(
            List.of(new StreamMetadata(STREAM_0, 0, 0, 200, StreamState.OPENED))));

        objectManager = Mockito.spy(MemoryMetadataManager.class);
        objectStorage = Mockito.spy(MemoryObjectStorage.class);
        final BlockingQueue<Pair<InvocationOnMock, CompletableFuture<ByteBuf>>> invocations = new LinkedBlockingQueue<>();
        doAnswer(invocation -> {
            CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
            invocations.add(Pair.of(invocation, cf));
            return cf;
        }).when(objectStorage).rangeRead(any(), Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());

        List<S3ObjectMetadata> s3ObjectMetadataList = new ArrayList<>();
        // stream data for object 0
        objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30)).thenAccept(objectId -> {
            assertEquals(OBJECT_0, objectId);
            ObjectWriter objectWriter = ObjectWriter.writer(objectId, objectStorage, 1024, 1024);
            StreamRecordBatch r1 = new StreamRecordBatch(STREAM_0, 0, 0, 80, TestUtils.random(80));
            objectWriter.write(STREAM_0, List.of(r1));
            objectWriter.close().join();
            List<StreamOffsetRange> streamsIndices = List.of(
                new StreamOffsetRange(STREAM_0, 0, 80)
            );
            S3ObjectMetadata objectMetadata = new S3ObjectMetadata(OBJECT_0, S3ObjectType.STREAM_SET, streamsIndices, System.currentTimeMillis(),
                System.currentTimeMillis(), objectWriter.size(), OBJECT_0);
            s3ObjectMetadataList.add(objectMetadata);
            r1.release();
        }).join();

        // stream data for object 1
        objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30)).thenAccept(objectId -> {
            assertEquals(OBJECT_1, objectId);
            ObjectWriter objectWriter = ObjectWriter.writer(OBJECT_1, objectStorage, 1024, 1024);
            StreamRecordBatch r2 = new StreamRecordBatch(STREAM_0, 0, 80, 120, TestUtils.random(120));
            objectWriter.write(STREAM_0, List.of(r2));
            objectWriter.close().join();
            List<StreamOffsetRange> streamsIndices = List.of(
                new StreamOffsetRange(STREAM_0, 80, 120)
            );
            S3ObjectMetadata objectMetadata = new S3ObjectMetadata(OBJECT_1, S3ObjectType.STREAM_SET, streamsIndices, System.currentTimeMillis(),
                System.currentTimeMillis(), objectWriter.size(), OBJECT_1);
            s3ObjectMetadataList.add(objectMetadata);
            r2.release();
        }).join();

        doReturn(CompletableFuture.completedFuture(s3ObjectMetadataList)).when(objectManager).getServerObjects();

        compactionManager = new CompactionManager(config, objectManager, streamManager, objectStorage);

        CompletableFuture<Void> cf = compactionManager.compact();
        Thread.sleep(3000);
        compactionManager.shutdown();

        AtomicBoolean done = new AtomicBoolean(false);
        CompletableFuture.runAsync(() -> {
            while (!done.get()) {
                Pair<InvocationOnMock, CompletableFuture<ByteBuf>> pair = null;
                try {
                    pair = invocations.poll(1, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (pair == null) {
                    continue;
                }
                CompletableFuture<ByteBuf> realCf = null;
                try {
                    realCf = (CompletableFuture<ByteBuf>) pair.getLeft().callRealMethod();
                    pair.getRight().complete(realCf.get());
                } catch (Throwable e) {
                    fail("Should not throw exception");
                }
            }
        });

        try {
            cf.join();
        } catch (Exception e) {
            fail("Should not throw exception");
        } finally {
            done.get();
        }
    }

    private boolean checkDataIntegrity(List<StreamMetadata> streamMetadataList, List<S3ObjectMetadata> s3ObjectMetadata,
        CommitStreamSetObjectRequest request) {
        Map<Long, S3ObjectMetadata> s3WALObjectMetadataMap = s3ObjectMetadata.stream()
            .collect(Collectors.toMap(S3ObjectMetadata::objectId, e -> e));
        Map<Long, List<StreamDataBlock>> streamDataBlocks = CompactionUtils.blockWaitObjectIndices(streamMetadataList, s3ObjectMetadata, objectStorage);
        for (Map.Entry<Long, List<StreamDataBlock>> entry : streamDataBlocks.entrySet()) {
            long objectId = entry.getKey();
            DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(objectId,
                s3WALObjectMetadataMap.get(objectId).objectSize(), S3ObjectType.STREAM_SET), objectStorage);
            reader.readBlocks(entry.getValue());
        }

        Map<Long, S3ObjectMetadata> compactedObjectMap = new HashMap<>();
        for (StreamObject streamObject : request.getStreamObjects()) {
            S3ObjectMetadata objectMetadata = new S3ObjectMetadata(streamObject.getObjectId(), S3ObjectType.STREAM,
                List.of(new StreamOffsetRange(streamObject.getStreamId(), streamObject.getStartOffset(), streamObject.getEndOffset())),
                System.currentTimeMillis(), System.currentTimeMillis(), streamObject.getObjectSize(), S3StreamConstant.INVALID_ORDER_ID);
            compactedObjectMap.put(streamObject.getObjectId(), objectMetadata);
        }
        List<StreamOffsetRange> streamOffsetRanges = new ArrayList<>();
        for (ObjectStreamRange objectStreamRange : request.getStreamRanges()) {
            streamOffsetRanges.add(new StreamOffsetRange(objectStreamRange.getStreamId(),
                objectStreamRange.getStartOffset(), objectStreamRange.getEndOffset()));
        }
        if (request.getObjectId() != -1) {
            S3ObjectMetadata metadata = new S3ObjectMetadata(request.getObjectId(), S3ObjectType.STREAM_SET,
                streamOffsetRanges, System.currentTimeMillis(), System.currentTimeMillis(), request.getObjectSize(), request.getOrderId());
            compactedObjectMap.put(request.getObjectId(), metadata);
        }

        Map<Long, List<StreamDataBlock>> compactedStreamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, new ArrayList<>(compactedObjectMap.values()), objectStorage);
        for (Map.Entry<Long, List<StreamDataBlock>> entry : compactedStreamDataBlocksMap.entrySet()) {
            long objectId = entry.getKey();
            DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(objectId,
                compactedObjectMap.get(objectId).objectSize(), S3ObjectType.STREAM_SET), objectStorage);
            reader.readBlocks(entry.getValue());
        }
        List<StreamDataBlock> expectedStreamDataBlocks = CompactionUtils.sortStreamRangePositions(streamDataBlocks);
        List<StreamDataBlock> compactedStreamDataBlocks = CompactionUtils.sortStreamRangePositions(compactedStreamDataBlocksMap);

        int i = 0;
        for (StreamDataBlock compactedStreamDataBlock : compactedStreamDataBlocks) {
            long currStreamId = compactedStreamDataBlock.getStreamId();
            long startOffset = compactedStreamDataBlock.getStartOffset();
            if (i == expectedStreamDataBlocks.size()) {
                return false;
            }
            List<StreamDataBlock> groupedStreamDataBlocks = new ArrayList<>();
            for (; i < expectedStreamDataBlocks.size(); i++) {
                StreamDataBlock expectedBlock = expectedStreamDataBlocks.get(i);

                if (startOffset == compactedStreamDataBlock.getEndOffset()) {
                    break;
                }
                if (currStreamId != expectedBlock.getStreamId()) {
                    return false;
                }
                if (startOffset != expectedBlock.getStartOffset()) {
                    return false;
                }
                if (expectedBlock.getEndOffset() > compactedStreamDataBlock.getEndOffset()) {
                    return false;
                }
                startOffset = expectedBlock.getEndOffset();
                groupedStreamDataBlocks.add(expectedBlock);
            }
            List<StreamDataBlock> compactedGroupedStreamDataBlocks = mergeStreamDataBlocksForGroup(List.of(groupedStreamDataBlocks));
            if (!compare(compactedStreamDataBlock, compactedGroupedStreamDataBlocks.get(0))) {
                return false;
            }
        }
        return true;
    }
}
