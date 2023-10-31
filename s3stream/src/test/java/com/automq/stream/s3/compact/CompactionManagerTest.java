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

package com.automq.stream.s3.compact;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.compact.objects.StreamDataBlock;
import com.automq.stream.s3.compact.operator.DataBlockReader;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.objects.CommitWALObjectRequest;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@Timeout(60)
@Tag("S3Unit")
public class CompactionManagerTest extends CompactionTestBase {
    private static final int BROKER0 = 0;
    private CompactionAnalyzer compactionAnalyzer;
    private CompactionManager compactionManager;
    private Config config;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        config = Mockito.mock(Config.class);
        when(config.brokerId()).thenReturn(BROKER0);
        when(config.s3ObjectCompactionNWInBandwidth()).thenReturn(500L);
        when(config.s3ObjectCompactionNWOutBandwidth()).thenReturn(500L);
        when(config.s3ObjectCompactionUploadConcurrency()).thenReturn(3);
        when(config.s3ObjectPartSize()).thenReturn(100);
        when(config.s3ObjectCompactionCacheSize()).thenReturn(300L);
        when(config.s3ObjectCompactionStreamSplitSize()).thenReturn(100L);
        when(config.s3ObjectCompactionForceSplitPeriod()).thenReturn(120);
        when(config.s3ObjectCompactionMaxObjectNum()).thenReturn(100);
        when(config.s3ObjectMaxStreamNumPerWAL()).thenReturn(100);
        when(config.s3ObjectMaxStreamObjectNumPerCommit()).thenReturn(100);
//        when(config.networkInboundBaselineBandwidth()).thenReturn(1000L);
        compactionAnalyzer = new CompactionAnalyzer(config.s3ObjectCompactionCacheSize(), config.s3ObjectCompactionStreamSplitSize(),
                config.s3ObjectMaxStreamNumPerWAL(), config.s3ObjectMaxStreamObjectNumPerCommit());
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
        when(config.s3ObjectCompactionForceSplitPeriod()).thenReturn(0);
        compactionManager = new CompactionManager(config, objectManager, streamManager, s3Operator);

        CommitWALObjectRequest request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(0));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_0), request.getCompactedObjectIds());
        Assertions.assertEquals(3, request.getStreamObjects().size());
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, Collections.singletonList(s3ObjectMetadata.get(0)), request));

        request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(1));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_1), request.getCompactedObjectIds());
        Assertions.assertEquals(2, request.getStreamObjects().size());
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, Collections.singletonList(s3ObjectMetadata.get(1)), request));

        request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(2));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_2), request.getCompactedObjectIds());
        Assertions.assertEquals(2, request.getStreamObjects().size());
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, Collections.singletonList(s3ObjectMetadata.get(2)), request));
    }

    @Test
    public void testForceSplitWithLimit() {
        when(config.s3ObjectCompactionCacheSize()).thenReturn(5L);
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        when(config.s3ObjectCompactionForceSplitPeriod()).thenReturn(0);
        compactionManager = new CompactionManager(config, objectManager, streamManager, s3Operator);

        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        CommitWALObjectRequest request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(0));
        Assertions.assertNull(request);
    }

    @Test
    public void testForceSplitWithLimit2() {
        when(config.s3ObjectCompactionCacheSize()).thenReturn(150L);
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        when(config.s3ObjectCompactionForceSplitPeriod()).thenReturn(0);
        compactionManager = new CompactionManager(config, objectManager, streamManager, s3Operator);

        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        CommitWALObjectRequest request = compactionManager.buildSplitRequest(streamMetadataList, s3ObjectMetadata.get(0));
        Assertions.assertEquals(-1, request.getObjectId());
        Assertions.assertEquals(List.of(OBJECT_0), request.getCompactedObjectIds());
        Assertions.assertEquals(3, request.getStreamObjects().size());
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, Collections.singletonList(s3ObjectMetadata.get(0)), request));
    }

    @Test
    public void testCompact() {
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        compactionManager = new CompactionManager(config, objectManager, streamManager, s3Operator);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        CommitWALObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, s3ObjectMetadata);

        assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_2));
        assertEquals(3, request.getStreamObjects().size());
        assertEquals(2, request.getStreamRanges().size());

        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, s3ObjectMetadata, request));
    }

    @Test
    public void testCompactWithDataTrimmed() {
        when(streamManager.getStreams(Collections.emptyList())).thenReturn(CompletableFuture.completedFuture(
                List.of(new StreamMetadata(STREAM_0, 0, 5, 20, StreamState.OPENED),
                        new StreamMetadata(STREAM_1, 0, 25, 500, StreamState.OPENED),
                        new StreamMetadata(STREAM_2, 0, 30, 270, StreamState.OPENED))));
        compactionManager = new CompactionManager(config, objectManager, streamManager, s3Operator);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        CommitWALObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST);

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
        compactionManager = new CompactionManager(config, objectManager, streamManager, s3Operator);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        CommitWALObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST);

        assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_2));
        assertEquals(3, request.getStreamObjects().size());
        assertEquals(2, request.getStreamRanges().size());

        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, request));
    }

    @Test
    public void testCompactWithNonExistStream() {
        when(streamManager.getStreams(Collections.emptyList())).thenReturn(CompletableFuture.completedFuture(
                List.of(new StreamMetadata(STREAM_1, 0, 25, 500, StreamState.OPENED),
                        new StreamMetadata(STREAM_2, 0, 30, 270, StreamState.OPENED))));
        compactionManager = new CompactionManager(config, objectManager, streamManager, s3Operator);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        CommitWALObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST);

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
    public void testCompactNoneExistObjects() {
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        compactionManager = new CompactionManager(config, objectManager, streamManager, s3Operator);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        Map<Long, List<StreamDataBlock>> streamDataBlockMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, s3ObjectMetadata, s3Operator);
        List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(streamDataBlockMap, new HashSet<>());
        s3Operator.delete(s3ObjectMetadata.get(0).key()).join();
        CommitWALObjectRequest request = new CommitWALObjectRequest();
        Assertions.assertThrowsExactly(IllegalArgumentException.class,
                () -> compactionManager.executeCompactionPlans(request, compactionPlans, s3ObjectMetadata));
    }

    private void testCompactWithNWInThrottle(long networkInThreshold) {
        when(config.s3ObjectCompactionNWInBandwidth()).thenReturn(networkInThreshold);
        compactionManager = new CompactionManager(config, objectManager, streamManager, s3Operator);

        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();

        long expectedMinCompleteTime = 530 / networkInThreshold;
        System.out.printf("Expect to complete no less than %ds%n", expectedMinCompleteTime);
        long start = System.currentTimeMillis();
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        CommitWALObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, s3ObjectMetadata);
        long cost = System.currentTimeMillis() - start;
        System.out.printf("Cost %ds%n", cost / 1000);
        assertTrue(cost > expectedMinCompleteTime * 1000);

        assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_2));
        assertEquals(3, request.getStreamObjects().size());
        assertEquals(2, request.getStreamRanges().size());

        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, s3ObjectMetadata, request));
    }

    @Test
    public void testCompactWithLimit() {
        when(config.s3ObjectCompactionStreamSplitSize()).thenReturn(70L);
        when(config.s3ObjectMaxStreamNumPerWAL()).thenReturn(MAX_STREAM_NUM_IN_WAL);
        when(config.s3ObjectMaxStreamObjectNumPerCommit()).thenReturn(2);
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        compactionManager = new CompactionManager(config, objectManager, streamManager, s3Operator);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        CommitWALObjectRequest request = compactionManager.buildCompactRequest(streamMetadataList, s3ObjectMetadata);

        assertEquals(List.of(OBJECT_0, OBJECT_1), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_2));
        assertEquals(2, request.getStreamObjects().size());
        assertEquals(1, request.getStreamRanges().size());

        Set<Long> compactedObjectIds = new HashSet<>(request.getCompactedObjectIds());
        s3ObjectMetadata = s3ObjectMetadata.stream().filter(s -> compactedObjectIds.contains(s.objectId())).toList();
        Assertions.assertTrue(checkDataIntegrity(streamMetadataList, s3ObjectMetadata, request));
    }

    private boolean checkDataIntegrity(List<StreamMetadata> streamMetadataList, List<S3ObjectMetadata> s3ObjectMetadata, CommitWALObjectRequest request) {
        Map<Long, S3ObjectMetadata> s3WALObjectMetadataMap = s3ObjectMetadata.stream()
                .collect(Collectors.toMap(S3ObjectMetadata::objectId, e -> e));
        Map<Long, List<StreamDataBlock>> streamDataBlocks = CompactionUtils.blockWaitObjectIndices(streamMetadataList, s3ObjectMetadata, s3Operator);
        for (Map.Entry<Long, List<StreamDataBlock>> entry : streamDataBlocks.entrySet()) {
            long objectId = entry.getKey();
            DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(objectId,
                    s3WALObjectMetadataMap.get(objectId).objectSize(), S3ObjectType.WAL), s3Operator);
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
            S3ObjectMetadata metadata = new S3ObjectMetadata(request.getObjectId(), S3ObjectType.WAL,
                    streamOffsetRanges, System.currentTimeMillis(), System.currentTimeMillis(), request.getObjectSize(), request.getOrderId());
            compactedObjectMap.put(request.getObjectId(), metadata);
        }

        Map<Long, List<StreamDataBlock>> compactedStreamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, new ArrayList<>(compactedObjectMap.values()), s3Operator);
        for (Map.Entry<Long, List<StreamDataBlock>> entry : compactedStreamDataBlocksMap.entrySet()) {
            long objectId = entry.getKey();
            DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(objectId,
                    compactedObjectMap.get(objectId).objectSize(), S3ObjectType.WAL), s3Operator);
            reader.readBlocks(entry.getValue());
        }
        List<StreamDataBlock> expectedStreamDataBlocks = streamDataBlocks.values().stream().flatMap(Collection::stream).toList();
        List<StreamDataBlock> compactedStreamDataBlocks = compactedStreamDataBlocksMap.values().stream().flatMap(Collection::stream).toList();
        if (expectedStreamDataBlocks.size() != compactedStreamDataBlocks.size()) {
            return false;
        }
        for (StreamDataBlock compactedStreamDataBlock : compactedStreamDataBlocks) {
            if (expectedStreamDataBlocks.stream().noneMatch(s -> compare(compactedStreamDataBlock, s))) {
                return false;
            }
        }
        return true;
    }

    @Test
    @Disabled
    public void testCompactWithNWInThrottle0() {
        testCompactWithNWInThrottle(20L);
    }

    @Test
    @Disabled
    public void testCompactWithNWInThrottle1() {
        testCompactWithNWInThrottle(50L);
    }

    @Test
    @Disabled
    public void testCompactWithNWInThrottle2() {
        testCompactWithNWInThrottle(200L);
    }
}
