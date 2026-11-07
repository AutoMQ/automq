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

package com.automq.stream.s3.compact;

import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.compact.objects.CompactedObject;
import com.automq.stream.s3.compact.objects.CompactedObjectBuilder;
import com.automq.stream.s3.compact.objects.CompactionType;
import com.automq.stream.s3.compact.utils.CompactionUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@Timeout(60)
@Tag("S3Unit")
public class CompactionAnalyzerTest extends CompactionTestBase {

    private static Map<Long, List<StreamDataBlock>> generateStreamDataBlocks() {
        return Map.of(
            OBJECT_0, List.of(
                new StreamDataBlock(STREAM_0, 0, 20, OBJECT_0, -1, 20, 1),
                new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, 30, 1),
                new StreamDataBlock(STREAM_2, 30, 60, OBJECT_0, -1, 30, 1)),
            OBJECT_1, List.of(
                new StreamDataBlock(STREAM_0, 20, 25, OBJECT_1, -1, 5, 1),
                new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, 60, 1)),
            OBJECT_2, List.of(
                new StreamDataBlock(STREAM_1, 400, 500, OBJECT_2, -1, 100, 1),
                new StreamDataBlock(STREAM_2, 230, 270, OBJECT_2, -1, 40, 1))
        );
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testReadObjectIndices() {
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, objectStorage);
        Map<Long, List<StreamDataBlock>> expectedBlocksMap = Map.of(
            OBJECT_0, List.of(
                new StreamDataBlock(STREAM_0, 0, 15, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 25, 30, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_2, 30, 60, OBJECT_0, -1, -1, 1)),
            OBJECT_1, List.of(
                new StreamDataBlock(STREAM_0, 15, 20, OBJECT_1, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1)),
            OBJECT_2, List.of(
                new StreamDataBlock(STREAM_1, 400, 500, OBJECT_2, -1, -1, 1),
                new StreamDataBlock(STREAM_2, 230, 270, OBJECT_2, -1, -1, 1)));
        assertTrue(compare(streamDataBlocksMap, expectedBlocksMap));
    }

    @Test
    public void testReadObjectIndicesWithTrimmedData() {
        when(streamManager.getStreams(Collections.emptyList())).thenReturn(CompletableFuture.completedFuture(
            List.of(new StreamMetadata(STREAM_0, 0, 15, 20, StreamState.OPENED),
                new StreamMetadata(STREAM_1, 0, 25, 500, StreamState.OPENED),
                new StreamMetadata(STREAM_2, 0, 30, 270, StreamState.OPENED))));
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, objectStorage);
        Map<Long, List<StreamDataBlock>> expectedBlocksMap = Map.of(
            OBJECT_0, List.of(
                new StreamDataBlock(STREAM_1, 25, 30, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_2, 30, 60, OBJECT_0, -1, -1, 1)),
            OBJECT_1, List.of(
                new StreamDataBlock(STREAM_0, 15, 20, OBJECT_1, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1)),
            OBJECT_2, List.of(
                new StreamDataBlock(STREAM_1, 400, 500, OBJECT_2, -1, -1, 1),
                new StreamDataBlock(STREAM_2, 230, 270, OBJECT_2, -1, -1, 1)));
        assertTrue(compare(streamDataBlocksMap, expectedBlocksMap));
    }

    @Test
    public void testFilterBlocksToCompact() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, STREAM_SPLIT_SIZE, MAX_STREAM_NUM_IN_WAL, MAX_STREAM_OBJECT_NUM);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, objectStorage);
        Map<Long, List<StreamDataBlock>> filteredMap = compactionAnalyzer.filterBlocksToCompact(streamDataBlocksMap);
        assertTrue(compare(filteredMap, streamDataBlocksMap));
    }

    @Test
    public void testFilterBlocksToCompact2() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, STREAM_SPLIT_SIZE, MAX_STREAM_NUM_IN_WAL, MAX_STREAM_OBJECT_NUM);
        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = Map.of(
            OBJECT_0, List.of(
                new StreamDataBlock(STREAM_0, 0, 20, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1)),
            OBJECT_1, List.of(
                new StreamDataBlock(STREAM_0, 20, 25, OBJECT_1, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1)),
            OBJECT_2, List.of(
                new StreamDataBlock(STREAM_2, 230, 270, OBJECT_2, -1, -1, 1)),
            OBJECT_3, List.of(
                new StreamDataBlock(STREAM_3, 0, 50, OBJECT_3, -1, -1, 1)));
        Map<Long, List<StreamDataBlock>> result = compactionAnalyzer.filterBlocksToCompact(streamDataBlocksMap);
        Map<Long, List<StreamDataBlock>> expectedBlocksMap = Map.of(
            OBJECT_0, List.of(
                new StreamDataBlock(STREAM_0, 0, 20, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1)),
            OBJECT_1, List.of(
                new StreamDataBlock(STREAM_0, 20, 25, OBJECT_1, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1)));
        assertTrue(compare(result, expectedBlocksMap));
    }

    @Test
    public void testSortStreamRangePositions() {
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, objectStorage);
        List<StreamDataBlock> sortedStreamDataBlocks = CompactionUtils.sortStreamRangePositions(streamDataBlocksMap);
        List<StreamDataBlock> expectedBlocks = List.of(
            new StreamDataBlock(STREAM_0, 0, 15, OBJECT_0, -1, -1, 1),
            new StreamDataBlock(STREAM_0, 15, 20, OBJECT_1, -1, -1, 1),
            new StreamDataBlock(STREAM_1, 25, 30, OBJECT_0, -1, -1, 1),
            new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1),
            new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1),
            new StreamDataBlock(STREAM_1, 400, 500, OBJECT_2, -1, -1, 1),
            new StreamDataBlock(STREAM_2, 30, 60, OBJECT_0, -1, -1, 1),
            new StreamDataBlock(STREAM_2, 230, 270, OBJECT_2, -1, -1, 1));
        for (int i = 0; i < sortedStreamDataBlocks.size(); i++) {
            assertTrue(compare(sortedStreamDataBlocks.get(i), expectedBlocks.get(i)));
        }
    }

    @Test
    public void testGroupObjectWithLimit() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, 100, MAX_STREAM_NUM_IN_WAL, MAX_STREAM_OBJECT_NUM);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, objectStorage);
        Set<Long> objectsToRemove = new HashSet<>();
        List<CompactedObjectBuilder> compactedObjectBuilders = compactionAnalyzer.groupObjectWithLimits(streamDataBlocksMap, objectsToRemove);
        Assertions.assertTrue(objectsToRemove.isEmpty());
        List<CompactedObjectBuilder> expectedCompactedObject = List.of(
            new CompactedObjectBuilder()
                .setType(CompactionType.COMPACT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 0, 15, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 15, 20, OBJECT_1, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 25, 30, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 400, 500, OBJECT_2, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_2, 30, 60, OBJECT_0, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.COMPACT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_2, 230, 270, OBJECT_2, -1, -1, 1)));
        for (int i = 0; i < compactedObjectBuilders.size(); i++) {
            assertTrue(compare(compactedObjectBuilders.get(i), expectedCompactedObject.get(i)));
        }
    }

    @Test
    public void testGroupObjectWithLimit2() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, 30, MAX_STREAM_NUM_IN_WAL, MAX_STREAM_OBJECT_NUM);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, objectStorage);
        Set<Long> objectsToRemove = new HashSet<>();
        List<CompactedObjectBuilder> compactedObjectBuilders = compactionAnalyzer.groupObjectWithLimits(streamDataBlocksMap, objectsToRemove);
        Assertions.assertTrue(objectsToRemove.isEmpty());
        List<CompactedObjectBuilder> expectedCompactedObject = List.of(
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 0, 15, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 15, 20, OBJECT_1, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 25, 30, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 400, 500, OBJECT_2, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_2, 30, 60, OBJECT_0, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_2, 230, 270, OBJECT_2, -1, -1, 1)));
        for (int i = 0; i < compactedObjectBuilders.size(); i++) {
            assertTrue(compare(compactedObjectBuilders.get(i), expectedCompactedObject.get(i)));
        }
    }

    @Test
    public void testGroupObjectWithLimit3() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, 70, MAX_STREAM_NUM_IN_WAL, 2);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, objectStorage);
        Set<Long> objectsToRemove = new HashSet<>();
        List<CompactedObjectBuilder> compactedObjectBuilders = compactionAnalyzer.groupObjectWithLimits(streamDataBlocksMap, objectsToRemove);
        Assertions.assertEquals(Set.of(OBJECT_2), objectsToRemove);
        List<CompactedObjectBuilder> expectedCompactedObject = List.of(
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 0, 15, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 15, 20, OBJECT_1, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 25, 30, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.COMPACT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_2, 30, 60, OBJECT_0, -1, -1, 1)));
        for (int i = 0; i < compactedObjectBuilders.size(); i++) {
            assertTrue(compare(compactedObjectBuilders.get(i), expectedCompactedObject.get(i)));
        }
    }

    @Test
    public void testGroupObjectWithLimit4() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, 50, MAX_STREAM_NUM_IN_WAL, 1);
        Set<Long> objectsToRemove = new HashSet<>();
        List<CompactedObjectBuilder> compactedObjectBuilders = compactionAnalyzer.groupObjectWithLimits(generateStreamDataBlocks(), objectsToRemove);
        Assertions.assertEquals(Set.of(OBJECT_2), objectsToRemove);
        List<CompactedObjectBuilder> expectedCompactedObject = List.of(
            new CompactedObjectBuilder()
                .setType(CompactionType.COMPACT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 0, 20, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 20, 25, OBJECT_1, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.COMPACT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_2, 30, 60, OBJECT_0, -1, -1, 1)));
        for (int i = 0; i < compactedObjectBuilders.size(); i++) {
            assertTrue(compare(compactedObjectBuilders.get(i), expectedCompactedObject.get(i)));
        }
    }

    @Test
    public void testGroupObjectWithLimit5() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, 50, 1, MAX_STREAM_OBJECT_NUM);
        Set<Long> objectsToRemove = new HashSet<>();
        List<CompactedObjectBuilder> compactedObjectBuilders = compactionAnalyzer.groupObjectWithLimits(generateStreamDataBlocks(), objectsToRemove);
        Assertions.assertEquals(Set.of(OBJECT_0, OBJECT_2), objectsToRemove);
        Assertions.assertTrue(compactedObjectBuilders.isEmpty());
    }

    @Test
    public void testGroupObjectWithLimit6() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, 29, MAX_STREAM_NUM_IN_WAL, 2);
        Set<Long> objectsToRemove = new HashSet<>();
        List<CompactedObjectBuilder> compactedObjectBuilders = compactionAnalyzer.groupObjectWithLimits(generateStreamDataBlocks(), objectsToRemove);
        Assertions.assertEquals(Set.of(OBJECT_2), objectsToRemove);
        List<CompactedObjectBuilder> expectedCompactedObject = List.of(
            new CompactedObjectBuilder()
                .setType(CompactionType.COMPACT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 0, 20, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 20, 25, OBJECT_1, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1)),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_2, 30, 60, OBJECT_0, -1, -1, 1)));
        for (int i = 0; i < compactedObjectBuilders.size(); i++) {
            assertTrue(compare(compactedObjectBuilders.get(i), expectedCompactedObject.get(i)));
        }
    }

    @Test
    public void testGroupObjectWithLimit7() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, 29, MAX_STREAM_NUM_IN_WAL, 1);
        Set<Long> objectsToRemove = new HashSet<>();
        List<CompactedObjectBuilder> compactedObjectBuilders = compactionAnalyzer.groupObjectWithLimits(generateStreamDataBlocks(), objectsToRemove);
        Assertions.assertEquals(Set.of(OBJECT_0, OBJECT_2), objectsToRemove);
        Assertions.assertTrue(compactedObjectBuilders.isEmpty());
    }

    @Test
    public void testCompactionPlans1() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, 100, MAX_STREAM_NUM_IN_WAL, MAX_STREAM_OBJECT_NUM);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, objectStorage);
        List<CompactionPlan> compactionPlans = compactionAnalyzer.analyze(streamDataBlocksMap, new HashSet<>());
        Assertions.assertEquals(1, compactionPlans.size());
        List<CompactedObject> expectCompactedObjects = List.of(
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 25, 30, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1))
                .build(),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 400, 500, OBJECT_2, -1, -1, 1))
                .build(),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_2, 30, 60, OBJECT_0, -1, -1, 1))
                .build(),
            new CompactedObjectBuilder()
                .setType(CompactionType.COMPACT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 0, 15, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 15, 20, OBJECT_1, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_2, 230, 270, OBJECT_2, -1, -1, 1))
                .build());
        Map<Long, List<StreamDataBlock>> expectObjectStreamDataBlocks = Map.of(
            OBJECT_0, List.of(
                new StreamDataBlock(STREAM_0, 0, 15, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 25, 30, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_2, 30, 60, OBJECT_0, -1, -1, 1)),
            OBJECT_1, List.of(
                new StreamDataBlock(STREAM_0, 15, 20, OBJECT_1, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1)),
            OBJECT_2, List.of(
                new StreamDataBlock(STREAM_1, 400, 500, OBJECT_2, -1, -1, 1),
                new StreamDataBlock(STREAM_2, 230, 270, OBJECT_2, -1, -1, 1)));
        CompactionPlan compactionPlan = compactionPlans.get(0);
        for (int i = 0; i < compactionPlan.compactedObjects().size(); i++) {
            assertTrue(compare(compactionPlan.compactedObjects().get(i), expectCompactedObjects.get(i)));
        }
        for (Long objectId : compactionPlan.streamDataBlocksMap().keySet()) {
            assertTrue(compare(compactionPlan.streamDataBlocksMap().get(objectId), expectObjectStreamDataBlocks.get(objectId)));
        }
    }

    private void checkCompactionPlan2(List<CompactionPlan> compactionPlans) {
        Assertions.assertEquals(2, compactionPlans.size());

        // first iteration
        List<CompactedObject> expectCompactedObjects = List.of(
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 25, 30, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1))
                .build(),
            new CompactedObjectBuilder()
                .setType(CompactionType.COMPACT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 0, 15, OBJECT_0, -1, -1, 1))
                .addStreamDataBlock(new StreamDataBlock(STREAM_0, 15, 20, OBJECT_1, -1, -1, 1))
                .build());
        Map<Long, List<StreamDataBlock>> expectObjectStreamDataBlocks = Map.of(
            OBJECT_0, List.of(
                new StreamDataBlock(STREAM_0, 0, 15, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 25, 30, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 30, 60, OBJECT_0, -1, -1, 1)),
            OBJECT_1, List.of(
                new StreamDataBlock(STREAM_0, 15, 20, OBJECT_1, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 60, 120, OBJECT_1, -1, -1, 1)));
        CompactionPlan compactionPlan = compactionPlans.get(0);
        for (int i = 0; i < compactionPlan.compactedObjects().size(); i++) {
            assertTrue(compare(compactionPlan.compactedObjects().get(i), expectCompactedObjects.get(i)));
        }
        for (Long objectId : compactionPlan.streamDataBlocksMap().keySet()) {
            assertTrue(compare(compactionPlan.streamDataBlocksMap().get(objectId), expectObjectStreamDataBlocks.get(objectId)));
        }

        // second iteration
        expectCompactedObjects = List.of(
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_1, 400, 500, OBJECT_2, -1, -1, 1))
                .build(),
            new CompactedObjectBuilder()
                .setType(CompactionType.SPLIT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_2, 30, 60, OBJECT_0, -1, -1, 1))
                .build(),
            new CompactedObjectBuilder()
                .setType(CompactionType.COMPACT)
                .addStreamDataBlock(new StreamDataBlock(STREAM_2, 230, 270, OBJECT_2, -1, -1, 1))
                .build());
        expectObjectStreamDataBlocks = Map.of(
            OBJECT_0, List.of(
                new StreamDataBlock(STREAM_2, 30, 60, OBJECT_0, -1, -1, 1)),
            OBJECT_2, List.of(
                new StreamDataBlock(STREAM_1, 400, 500, OBJECT_2, -1, -1, 1),
                new StreamDataBlock(STREAM_2, 230, 270, OBJECT_2, -1, -1, 1)));
        compactionPlan = compactionPlans.get(1);
        for (int i = 0; i < compactionPlan.compactedObjects().size(); i++) {
            assertTrue(compare(compactionPlan.compactedObjects().get(i), expectCompactedObjects.get(i)));
        }
        for (Long objectId : compactionPlan.streamDataBlocksMap().keySet()) {
            assertTrue(compare(compactionPlan.streamDataBlocksMap().get(objectId), expectObjectStreamDataBlocks.get(objectId)));
        }
    }

    @Test
    public void testCompactionPlans2() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(300, 100, MAX_STREAM_NUM_IN_WAL, MAX_STREAM_OBJECT_NUM);
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, S3_WAL_OBJECT_METADATA_LIST, objectStorage);
        List<CompactionPlan> compactionPlans = compactionAnalyzer.analyze(streamDataBlocksMap, new HashSet<>());
        checkCompactionPlan2(compactionPlans);
    }

    @Test
    public void testCompactionPlansWithInvalidObject() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(300, 100, MAX_STREAM_NUM_IN_WAL, MAX_STREAM_OBJECT_NUM);
        List<S3ObjectMetadata> s3ObjectMetadata = new ArrayList<>(S3_WAL_OBJECT_METADATA_LIST);
        s3ObjectMetadata.add(
            new S3ObjectMetadata(100, S3ObjectType.STREAM_SET,
                List.of(new StreamOffsetRange(STREAM_2, 1000, 1200)), System.currentTimeMillis(),
                System.currentTimeMillis(), 512, 100));
        List<StreamMetadata> streamMetadataList = this.streamManager.getStreams(Collections.emptyList()).join();
        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, s3ObjectMetadata, objectStorage);
        List<CompactionPlan> compactionPlans = compactionAnalyzer.analyze(streamDataBlocksMap, new HashSet<>());
        checkCompactionPlan2(compactionPlans);
    }
}
