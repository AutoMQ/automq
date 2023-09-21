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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        when(config.s3ObjectCompactionExecutionScoreThreshold()).thenReturn(0.5);
        when(config.s3ObjectCompactionStreamSplitSize()).thenReturn(100L);
        when(config.s3ObjectCompactionForceSplitPeriod()).thenReturn(120);
        when(config.s3ObjectCompactionMaxObjectNum()).thenReturn(100);
        compactionAnalyzer = new CompactionAnalyzer(config.s3ObjectCompactionCacheSize(),
                config.s3ObjectCompactionExecutionScoreThreshold(), config.s3ObjectCompactionStreamSplitSize());
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();
        if (compactionManager != null) {
            compactionManager.shutdown();
        }
    }

    @Test
    public void testFilterS3Object() {
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        when(config.s3ObjectCompactionMaxObjectNum()).thenReturn(2);
        CompactionManager compactionManager = new CompactionManager(config, objectManager, s3Operator);
        Map<Boolean, List<S3ObjectMetadata>> filteredMap = compactionManager.filterS3Objects(s3ObjectMetadata);
        for (List<S3ObjectMetadata> list : filteredMap.values()) {
            assertTrue(list.size() <= 2);
        }
    }

    @Test
    public void testForceSplit() {
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        List<StreamDataBlock> streamDataBlocks = CompactionUtils.blockWaitObjectIndices(s3ObjectMetadata, s3Operator)
                .values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        compactionManager = new CompactionManager(config, objectManager, s3Operator);
        List<CompletableFuture<StreamObject>> cfList = compactionManager.splitWALObjects(s3ObjectMetadata);
        List<StreamObject> streamObjects = cfList.stream().map(CompletableFuture::join).collect(Collectors.toList());

        for (StreamObject streamObject : streamObjects) {
            StreamDataBlock streamDataBlock = get(streamDataBlocks, streamObject);
            Assertions.assertNotNull(streamDataBlock);
            assertEquals(calculateObjectSize(List.of(streamDataBlock)), streamObject.getObjectSize());

            DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(streamObject.getObjectId(),
                    streamObject.getObjectSize(), S3ObjectType.STREAM), s3Operator);
            reader.parseDataBlockIndex();
            List<StreamDataBlock> streamDataBlocksFromS3 = reader.getDataBlockIndex().join();
            assertEquals(1, streamDataBlocksFromS3.size());
            assertEquals(0, streamDataBlocksFromS3.get(0).getBlockStartPosition());
            assertTrue(contains(streamDataBlocks, streamDataBlocksFromS3.get(0)));
        }
    }

    private StreamDataBlock get(List<StreamDataBlock> streamDataBlocks, StreamObject streamObject) {
        for (StreamDataBlock block : streamDataBlocks) {
            if (block.getStreamId() == streamObject.getStreamId() &&
                    block.getStartOffset() == streamObject.getStartOffset() &&
                    block.getEndOffset() == streamObject.getEndOffset()) {
                return block;
            }
        }
        return null;
    }

    private boolean contains(List<StreamDataBlock> streamDataBlocks, StreamDataBlock streamDataBlock) {
        for (StreamDataBlock block : streamDataBlocks) {
            if (block.getStreamId() == streamDataBlock.getStreamId() &&
                    block.getStartOffset() == streamDataBlock.getStartOffset() &&
                    block.getEndOffset() == streamDataBlock.getEndOffset()) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void testCompact() {
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        compactionManager = new CompactionManager(config, objectManager, s3Operator);
        CommitWALObjectRequest request = compactionManager.buildCompactRequest(s3ObjectMetadata);

        assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_2));
        assertEquals(3, request.getStreamObjects().size());
        assertEquals(2, request.getStreamRanges().size());

        Assertions.assertTrue(checkDataIntegrity(s3ObjectMetadata, request));
    }

    @Test
    public void testCompactWithDataTrimmed() {
        List<S3ObjectMetadata> objectMetadataList = new ArrayList<>();
        for (int i = 0; i < S3_WAL_OBJECT_METADATA_LIST.size(); i++) {
            S3ObjectMetadata s3ObjectMetadata = S3_WAL_OBJECT_METADATA_LIST.get(i);
            if (i == 0) {
                List<StreamOffsetRange> streamOffsetRangeList = new ArrayList<>();
                streamOffsetRangeList.addAll(s3ObjectMetadata.getOffsetRanges().subList(0, 1));
                streamOffsetRangeList.addAll(s3ObjectMetadata.getOffsetRanges().subList(2, s3ObjectMetadata.getOffsetRanges().size()));
                s3ObjectMetadata = new S3ObjectMetadata(s3ObjectMetadata.objectId(), s3ObjectMetadata.getType(), streamOffsetRangeList,
                        s3ObjectMetadata.dataTimeInMs(), s3ObjectMetadata.committedTimestamp(), s3ObjectMetadata.objectSize(), s3ObjectMetadata.getOrderId());
            }
            objectMetadataList.add(s3ObjectMetadata);
        }
        compactionManager = new CompactionManager(config, objectManager, s3Operator);
        CommitWALObjectRequest request = compactionManager.buildCompactRequest(objectMetadataList);

        assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_2));
        assertEquals(3, request.getStreamObjects().size());
        assertEquals(2, request.getStreamRanges().size());

        Assertions.assertTrue(checkDataIntegrity(objectMetadataList, request));
    }

    @Test
    public void testCompactNoneExistObjects() {
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        compactionManager = new CompactionManager(config, objectManager, s3Operator);
        Map<Long, List<StreamDataBlock>> streamDataBlockMap = CompactionUtils.blockWaitObjectIndices(s3ObjectMetadata, s3Operator);
        List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(streamDataBlockMap);
        s3Operator.delete(s3ObjectMetadata.get(0).key()).join();
        CommitWALObjectRequest request = new CommitWALObjectRequest();
        Assertions.assertThrowsExactly(IllegalArgumentException.class,
                () -> compactionManager.compactWALObjects(request, compactionPlans, s3ObjectMetadata));
    }

    private void testCompactWithNWInThrottle(long networkInThreshold) {
        when(config.s3ObjectCompactionNWInBandwidth()).thenReturn(networkInThreshold);
        compactionManager = new CompactionManager(config, objectManager, s3Operator);

        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();

        long expectedMinCompleteTime = 530 / networkInThreshold;
        System.out.printf("Expect to complete no less than %ds%n", expectedMinCompleteTime);
        long start = System.currentTimeMillis();
        CommitWALObjectRequest request = compactionManager.buildCompactRequest(s3ObjectMetadata);
        long cost = System.currentTimeMillis() - start;
        System.out.printf("Cost %ds%n", cost / 1000);
        assertTrue(cost > expectedMinCompleteTime * 1000);

        assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        assertEquals(OBJECT_0, request.getOrderId());
        assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> assertTrue(s.getObjectId() > OBJECT_2));
        assertEquals(3, request.getStreamObjects().size());
        assertEquals(2, request.getStreamRanges().size());

        Assertions.assertTrue(checkDataIntegrity(s3ObjectMetadata, request));
    }

    private boolean checkDataIntegrity(List<S3ObjectMetadata> s3ObjectMetadata, CommitWALObjectRequest request) {
        Map<Long, S3ObjectMetadata> s3WALObjectMetadataMap = s3ObjectMetadata.stream()
                .collect(Collectors.toMap(S3ObjectMetadata::objectId, e -> e));
        Map<Long, List<StreamDataBlock>> streamDataBlocks = CompactionUtils.blockWaitObjectIndices(s3ObjectMetadata, s3Operator);
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
        S3ObjectMetadata metadata = new S3ObjectMetadata(request.getObjectId(), S3ObjectType.WAL,
                streamOffsetRanges, System.currentTimeMillis(), System.currentTimeMillis(), request.getObjectSize(), request.getOrderId());
        compactedObjectMap.put(request.getObjectId(), metadata);
        Map<Long, List<StreamDataBlock>> compactedStreamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(new ArrayList<>(compactedObjectMap.values()), s3Operator);
        for (Map.Entry<Long, List<StreamDataBlock>> entry : compactedStreamDataBlocksMap.entrySet()) {
            long objectId = entry.getKey();
            DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(objectId,
                    compactedObjectMap.get(objectId).objectSize(), S3ObjectType.WAL), s3Operator);
            reader.readBlocks(entry.getValue());
        }
        List<StreamDataBlock> expectedStreamDataBlocks = streamDataBlocks.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        List<StreamDataBlock> compactedStreamDataBlocks = compactedStreamDataBlocksMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
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
    public void testCompactWithNWInThrottle0() {
        testCompactWithNWInThrottle(20L);
    }

    @Test
    public void testCompactWithNWInThrottle1() {
        testCompactWithNWInThrottle(50L);
    }

    @Test
    public void testCompactWithNWInThrottle2() {
        testCompactWithNWInThrottle(200L);
    }
}
