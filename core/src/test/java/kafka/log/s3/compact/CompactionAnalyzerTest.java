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

package kafka.log.s3.compact;

import kafka.log.s3.compact.objects.CompactedObject;
import kafka.log.s3.compact.objects.CompactedObjectBuilder;
import kafka.log.s3.compact.objects.CompactionType;
import kafka.log.s3.compact.objects.StreamDataBlock;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.metadata.stream.S3WALObjectMetadata;
import org.apache.kafka.metadata.stream.StreamOffsetRange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Timeout(30)
@Tag("S3Unit")
public class CompactionAnalyzerTest extends CompactionTestBase {

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
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, EXECUTION_SCORE_THRESHOLD, STREAM_SPLIT_SIZE, s3Operator);
        List<StreamDataBlock> streamDataBlocks = compactionAnalyzer.blockWaitObjectIndices(S3_WAL_OBJECT_METADATA_LIST);
        List<StreamDataBlock> expectedBlocks = List.of(
                new StreamDataBlock(STREAM_0, 0, 20, 0, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 30, 60, 1, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_2, 30, 60, 2, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_0, 20, 25, 0, OBJECT_1, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 60, 120, 1, OBJECT_1, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 400, 500, 0, OBJECT_2, -1, -1, 1),
                new StreamDataBlock(STREAM_2, 230, 270, 1, OBJECT_2, -1, -1, 1));
        for (int i = 0; i < streamDataBlocks.size(); i++) {
            Assertions.assertTrue(compare(streamDataBlocks.get(i), expectedBlocks.get(i)));
        }
    }

    @Test
    public void testFilterBlocksToCompact() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, EXECUTION_SCORE_THRESHOLD, STREAM_SPLIT_SIZE, s3Operator);
        List<StreamDataBlock> streamDataBlocks = compactionAnalyzer.blockWaitObjectIndices(S3_WAL_OBJECT_METADATA_LIST);
        List<StreamDataBlock> filteredBlocks = compactionAnalyzer.filterBlocksToCompact(streamDataBlocks);
        for (int i = 0; i < streamDataBlocks.size(); i++) {
            Assertions.assertTrue(compare(streamDataBlocks.get(i), filteredBlocks.get(i)));
        }
    }

    @Test
    public void testFilterBlocksToCompact2() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, EXECUTION_SCORE_THRESHOLD, STREAM_SPLIT_SIZE, s3Operator);
        List<StreamDataBlock> streamDataBlocks = List.of(
                new StreamDataBlock(STREAM_0, 0, 20, 0, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_0, 20, 25, 0, OBJECT_1, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 30, 60, 1, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 60, 120, 1, OBJECT_1, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 400, 500, 2, OBJECT_1, -1, -1, 1),
                new StreamDataBlock(STREAM_2, 30, 60, 0, OBJECT_2, -1, -1, 1),
                new StreamDataBlock(STREAM_2, 230, 270, 1, OBJECT_2, -1, -1, 1));
        List<StreamDataBlock> filteredBlocks = compactionAnalyzer.filterBlocksToCompact(streamDataBlocks);
        Assertions.assertEquals(5, filteredBlocks.size());
        for (int i = 0; i < filteredBlocks.size(); i++) {
            Assertions.assertTrue(compare(streamDataBlocks.get(i), filteredBlocks.get(i)));
        }
    }

    @Test
    public void testSortStreamRangePositions() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, EXECUTION_SCORE_THRESHOLD, STREAM_SPLIT_SIZE, s3Operator);
        List<StreamDataBlock> streamDataBlocks = compactionAnalyzer.blockWaitObjectIndices(S3_WAL_OBJECT_METADATA_LIST);
        List<StreamDataBlock> sortedStreamDataBlocks = compactionAnalyzer.sortStreamRangePositions(streamDataBlocks);
        List<StreamDataBlock> expectedBlocks = List.of(
                new StreamDataBlock(STREAM_0, 0, 20, 0, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_0, 20, 25, 0, OBJECT_1, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 30, 60, 1, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 60, 120, 1, OBJECT_1, -1, -1, 1),
                new StreamDataBlock(STREAM_1, 400, 500, 0, OBJECT_2, -1, -1, 1),
                new StreamDataBlock(STREAM_2, 30, 60, 2, OBJECT_0, -1, -1, 1),
                new StreamDataBlock(STREAM_2, 230, 270, 1, OBJECT_2, -1, -1, 1));
        for (int i = 0; i < sortedStreamDataBlocks.size(); i++) {
            Assertions.assertTrue(compare(sortedStreamDataBlocks.get(i), expectedBlocks.get(i)));
        }
    }

    @Test
    public void testBuildCompactedObject1() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, EXECUTION_SCORE_THRESHOLD, 100, s3Operator);
        List<CompactedObjectBuilder> compactedObjectBuilders = compactionAnalyzer.buildCompactedObjects(S3_WAL_OBJECT_METADATA_LIST);
        List<CompactedObjectBuilder> expectedCompactedObject = List.of(
                new CompactedObjectBuilder()
                        .setType(CompactionType.COMPACT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_0, 0, 20, 0, OBJECT_0, -1, -1, 1))
                        .addStreamDataBlock(new StreamDataBlock(STREAM_0, 20, 25, 0, OBJECT_1, -1, -1, 1)),
                new CompactedObjectBuilder()
                        .setType(CompactionType.SPLIT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_1, 30, 60, 1, OBJECT_0, -1, -1, 1))
                        .addStreamDataBlock(new StreamDataBlock(STREAM_1, 60, 120, 1, OBJECT_1, -1, -1, 1)),
                new CompactedObjectBuilder()
                        .setType(CompactionType.SPLIT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_1, 400, 500, 0, OBJECT_2, -1, -1, 1)),
                new CompactedObjectBuilder()
                        .setType(CompactionType.SPLIT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_2, 30, 60, 2, OBJECT_0, -1, -1, 1)),
                new CompactedObjectBuilder()
                        .setType(CompactionType.COMPACT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_2, 230, 270, 1, OBJECT_2, -1, -1, 1)));
        for (int i = 0; i < compactedObjectBuilders.size(); i++) {
            Assertions.assertTrue(compare(compactedObjectBuilders.get(i), expectedCompactedObject.get(i)));
        }
    }

    @Test
    public void testBuildCompactedObject2() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, EXECUTION_SCORE_THRESHOLD, 30, s3Operator);
        List<CompactedObjectBuilder> compactedObjectBuilders = compactionAnalyzer.buildCompactedObjects(S3_WAL_OBJECT_METADATA_LIST);
        List<CompactedObjectBuilder> expectedCompactedObject = List.of(
                new CompactedObjectBuilder()
                        .setType(CompactionType.SPLIT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_0, 0, 20, 0, OBJECT_0, -1, -1, 1))
                        .addStreamDataBlock(new StreamDataBlock(STREAM_0, 20, 25, 0, OBJECT_1, -1, -1, 1)),
                new CompactedObjectBuilder()
                        .setType(CompactionType.SPLIT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_1, 30, 60, 1, OBJECT_0, -1, -1, 1))
                        .addStreamDataBlock(new StreamDataBlock(STREAM_1, 60, 120, 1, OBJECT_1, -1, -1, 1)),
                new CompactedObjectBuilder()
                        .setType(CompactionType.SPLIT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_1, 400, 500, 0, OBJECT_2, -1, -1, 1)),
                new CompactedObjectBuilder()
                        .setType(CompactionType.SPLIT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_2, 30, 60, 2, OBJECT_0, -1, -1, 1)),
                new CompactedObjectBuilder()
                        .setType(CompactionType.COMPACT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_2, 230, 270, 1, OBJECT_2, -1, -1, 1)));
        for (int i = 0; i < compactedObjectBuilders.size(); i++) {
            Assertions.assertTrue(compare(compactedObjectBuilders.get(i), expectedCompactedObject.get(i)));
        }
    }

    @Test
    public void testCompactionPlans1() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, EXECUTION_SCORE_THRESHOLD, 100, s3Operator);
        List<CompactionPlan> compactionPlans = compactionAnalyzer.analyze(S3_WAL_OBJECT_METADATA_LIST);
        Assertions.assertEquals(1, compactionPlans.size());
        List<CompactedObject> expectCompactedObjects = List.of(
                new CompactedObjectBuilder()
                        .setType(CompactionType.SPLIT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_1, 30, 60, 1, OBJECT_0, -1, -1, 1))
                        .addStreamDataBlock(new StreamDataBlock(STREAM_1, 60, 120, 1, OBJECT_1, -1, -1, 1))
                        .build(),
                new CompactedObjectBuilder()
                        .setType(CompactionType.SPLIT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_1, 400, 500, 0, OBJECT_2, -1, -1, 1))
                        .build(),
                new CompactedObjectBuilder()
                        .setType(CompactionType.SPLIT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_2, 30, 60, 2, OBJECT_0, -1, -1, 1))
                        .build(),
                new CompactedObjectBuilder()
                        .setType(CompactionType.COMPACT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_0, 0, 20, 0, OBJECT_0, -1, -1, 1))
                        .addStreamDataBlock(new StreamDataBlock(STREAM_0, 20, 25, 0, OBJECT_1, -1, -1, 1))
                        .addStreamDataBlock(new StreamDataBlock(STREAM_2, 230, 270, 1, OBJECT_2, -1, -1, 1))
                        .build());
        Map<Long, List<StreamDataBlock>> expectObjectStreamDataBlocks = Map.of(
                OBJECT_0, List.of(
                        new StreamDataBlock(STREAM_0, 0, 20, 0, OBJECT_0, -1, -1, 1),
                        new StreamDataBlock(STREAM_1, 30, 60, 1, OBJECT_0, -1, -1, 1),
                        new StreamDataBlock(STREAM_2, 30, 60, 2, OBJECT_0, -1, -1, 1)),
                OBJECT_1, List.of(
                        new StreamDataBlock(STREAM_0, 20, 25, 0, OBJECT_1, -1, -1, 1),
                        new StreamDataBlock(STREAM_1, 60, 120, 1, OBJECT_1, -1, -1, 1)),
                OBJECT_2, List.of(
                        new StreamDataBlock(STREAM_1, 400, 500, 0, OBJECT_2, -1, -1, 1),
                        new StreamDataBlock(STREAM_2, 230, 270, 1, OBJECT_2, -1, -1, 1)));
        CompactionPlan compactionPlan = compactionPlans.get(0);
        for (int i = 0; i < compactionPlan.compactedObjects().size(); i++) {
            Assertions.assertTrue(compare(compactionPlan.compactedObjects().get(i), expectCompactedObjects.get(i)));
        }
        for (Long objectId : compactionPlan.streamDataBlocksMap().keySet()) {
            Assertions.assertTrue(compare(compactionPlan.streamDataBlocksMap().get(objectId), expectObjectStreamDataBlocks.get(objectId)));
        }
    }

    private void checkCompactionPlan2(List<CompactionPlan> compactionPlans) {
        Assertions.assertEquals(2, compactionPlans.size());

        // first iteration
        List<CompactedObject> expectCompactedObjects = List.of(
                new CompactedObjectBuilder()
                        .setType(CompactionType.SPLIT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_1, 30, 60, 1, OBJECT_0, -1, -1, 1))
                        .addStreamDataBlock(new StreamDataBlock(STREAM_1, 60, 120, 1, OBJECT_1, -1, -1, 1))
                        .build(),
                new CompactedObjectBuilder()
                        .setType(CompactionType.COMPACT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_0, 0, 20, 0, OBJECT_0, -1, -1, 1))
                        .addStreamDataBlock(new StreamDataBlock(STREAM_0, 20, 25, 0, OBJECT_1, -1, -1, 1))
                        .build());
        Map<Long, List<StreamDataBlock>> expectObjectStreamDataBlocks = Map.of(
                OBJECT_0, List.of(
                        new StreamDataBlock(STREAM_0, 0, 20, 0, OBJECT_0, -1, -1, 1),
                        new StreamDataBlock(STREAM_1, 30, 60, 1, OBJECT_0, -1, -1, 1)),
                OBJECT_1, List.of(
                        new StreamDataBlock(STREAM_0, 20, 25, 0, OBJECT_1, -1, -1, 1),
                        new StreamDataBlock(STREAM_1, 60, 120, 1, OBJECT_1, -1, -1, 1)));
        CompactionPlan compactionPlan = compactionPlans.get(0);
        for (int i = 0; i < compactionPlan.compactedObjects().size(); i++) {
            Assertions.assertTrue(compare(compactionPlan.compactedObjects().get(i), expectCompactedObjects.get(i)));
        }
        for (Long objectId : compactionPlan.streamDataBlocksMap().keySet()) {
            Assertions.assertTrue(compare(compactionPlan.streamDataBlocksMap().get(objectId), expectObjectStreamDataBlocks.get(objectId)));
        }

        // second iteration
        expectCompactedObjects = List.of(
                new CompactedObjectBuilder()
                        .setType(CompactionType.SPLIT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_1, 400, 500, 0, OBJECT_2, -1, -1, 1))
                        .build(),
                new CompactedObjectBuilder()
                        .setType(CompactionType.SPLIT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_2, 30, 60, 2, OBJECT_0, -1, -1, 1))
                        .build(),
                new CompactedObjectBuilder()
                        .setType(CompactionType.COMPACT)
                        .addStreamDataBlock(new StreamDataBlock(STREAM_2, 230, 270, 1, OBJECT_2, -1, -1, 1))
                        .build());
        expectObjectStreamDataBlocks = Map.of(
                OBJECT_0, List.of(
                        new StreamDataBlock(STREAM_2, 30, 60, 2, OBJECT_0, -1, -1, 1)),
                OBJECT_2, List.of(
                        new StreamDataBlock(STREAM_1, 400, 500, 0, OBJECT_2, -1, -1, 1),
                        new StreamDataBlock(STREAM_2, 230, 270, 1, OBJECT_2, -1, -1, 1)));
        compactionPlan = compactionPlans.get(1);
        for (int i = 0; i < compactionPlan.compactedObjects().size(); i++) {
            Assertions.assertTrue(compare(compactionPlan.compactedObjects().get(i), expectCompactedObjects.get(i)));
        }
        for (Long objectId : compactionPlan.streamDataBlocksMap().keySet()) {
            Assertions.assertTrue(compare(compactionPlan.streamDataBlocksMap().get(objectId), expectObjectStreamDataBlocks.get(objectId)));
        }
    }

    @Test
    public void testCompactionPlans2() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(300, EXECUTION_SCORE_THRESHOLD, 100, s3Operator);
        List<CompactionPlan> compactionPlans = compactionAnalyzer.analyze(S3_WAL_OBJECT_METADATA_LIST);
        checkCompactionPlan2(compactionPlans);
    }

    @Test
    public void testCompactionPlansWithInvalidObject() {
        CompactionAnalyzer compactionAnalyzer = new CompactionAnalyzer(300, EXECUTION_SCORE_THRESHOLD, 100, s3Operator);
        List<S3WALObjectMetadata> s3ObjectMetadata = new ArrayList<>(S3_WAL_OBJECT_METADATA_LIST);
        s3ObjectMetadata.add(new S3WALObjectMetadata(new S3WALObject(100, 0, Map.of(
                STREAM_2, List.of(new StreamOffsetRange(STREAM_2, 1000, 1200))
        ), 0), new S3ObjectMetadata(100, 0, S3ObjectType.WAL)));
        List<CompactionPlan> compactionPlans = compactionAnalyzer.analyze(s3ObjectMetadata);
        checkCompactionPlan2(compactionPlans);
    }
}
