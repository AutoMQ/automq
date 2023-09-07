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

import kafka.log.s3.ObjectWriter;
import kafka.log.s3.TestUtils;
import kafka.log.s3.compact.objects.CompactedObject;
import kafka.log.s3.compact.objects.CompactedObjectBuilder;
import kafka.log.s3.compact.objects.CompactionType;
import kafka.log.s3.compact.objects.StreamDataBlock;
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.operator.MemoryS3Operator;
import kafka.log.s3.operator.S3Operator;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Tag("S3Unit")
public class CompactionAnalyzerTest {
    private static final int BROKER_0 = 0;
    private static final long STREAM_0 = 0;
    private static final long STREAM_1 = 1;
    private static final long STREAM_2 = 2;
    private static final long OBJECT_0 = 0;
    private static final long OBJECT_1 = 1;
    private static final long OBJECT_2 = 2;
    private static final long CACHE_SIZE = 1024;
    private static final double EXECUTION_SCORE_THRESHOLD = 0.5;
    private static final long STREAM_SPLIT_SIZE = 30;
    private static final List<S3WALObjectMetadata> S3_WAL_OBJECT_METADATA_LIST = new ArrayList<>();
    private CompactionAnalyzer compactionAnalyzer;
    private S3Operator s3Operator;

    @BeforeEach
    public void setUp() throws Exception {
        s3Operator = new MemoryS3Operator();
        // stream data for object 0
        ObjectWriter objectWriter = new ObjectWriter(OBJECT_0, s3Operator, 1024, 1024);
        StreamRecordBatch r1 = new StreamRecordBatch(STREAM_0, 0, 0, 20, TestUtils.random(20));
        StreamRecordBatch r2 = new StreamRecordBatch(STREAM_1, 0, 30, 30, TestUtils.random(30));
        StreamRecordBatch r3 = new StreamRecordBatch(STREAM_2, 0, 30, 30, TestUtils.random(30));
        objectWriter.write(STREAM_0, List.of(r1));
        objectWriter.write(STREAM_1, List.of(r2));
        objectWriter.write(STREAM_2, List.of(r3));
        objectWriter.close().get();
        S3ObjectMetadata objectMetadata = new S3ObjectMetadata(OBJECT_0, objectWriter.size(), S3ObjectType.WAL);
        Map<Long, List<StreamOffsetRange>> streamsIndex = Map.of(
                (long) STREAM_0, List.of(new StreamOffsetRange(STREAM_0, 0, 20)),
                (long) STREAM_1, List.of(new StreamOffsetRange(STREAM_1, 30, 60)),
                (long) STREAM_2, List.of(new StreamOffsetRange(STREAM_2, 30, 60))
        );
        S3WALObject walObject = new S3WALObject(OBJECT_0, BROKER_0, streamsIndex, OBJECT_0);
        S3_WAL_OBJECT_METADATA_LIST.add(new S3WALObjectMetadata(walObject, objectMetadata));

        // stream data for object 1
        objectWriter = new ObjectWriter(OBJECT_1, s3Operator, 1024, 1024);
        StreamRecordBatch r4 = new StreamRecordBatch(STREAM_0, 0, 20, 5, TestUtils.random(5));
        StreamRecordBatch r5 = new StreamRecordBatch(STREAM_1, 0, 60, 60, TestUtils.random(60));
        objectWriter.write(STREAM_0, List.of(r4));
        objectWriter.write(STREAM_1, List.of(r5));
        objectWriter.close().get();
        objectMetadata = new S3ObjectMetadata(OBJECT_1, objectWriter.size(), S3ObjectType.WAL);
        streamsIndex = Map.of(
                (long) STREAM_0, List.of(new StreamOffsetRange(STREAM_0, 20, 25)),
                (long) STREAM_1, List.of(new StreamOffsetRange(STREAM_1, 60, 120))
        );
        walObject = new S3WALObject(OBJECT_1, BROKER_0, streamsIndex, OBJECT_1);
        S3_WAL_OBJECT_METADATA_LIST.add(new S3WALObjectMetadata(walObject, objectMetadata));

        // stream data for object 0
        objectWriter = new ObjectWriter(OBJECT_2, s3Operator, 1024, 1024);
        // redundant record
        StreamRecordBatch r6 = new StreamRecordBatch(STREAM_1, 0, 260, 20, TestUtils.random(20));
        StreamRecordBatch r7 = new StreamRecordBatch(STREAM_1, 0, 400, 100, TestUtils.random(100));
        StreamRecordBatch r8 = new StreamRecordBatch(STREAM_2, 0, 230, 40, TestUtils.random(40));
        objectWriter.write(STREAM_1, List.of(r6));
        objectWriter.write(STREAM_1, List.of(r7));
        objectWriter.write(STREAM_2, List.of(r8));
        objectWriter.close().get();
        objectMetadata = new S3ObjectMetadata(OBJECT_2, objectWriter.size(), S3ObjectType.WAL);
        streamsIndex = Map.of(
                (long) STREAM_1, List.of(new StreamOffsetRange(STREAM_1, 400, 500)),
                (long) STREAM_2, List.of(new StreamOffsetRange(STREAM_2, 230, 270))
        );
        walObject = new S3WALObject(OBJECT_2, BROKER_0, streamsIndex, OBJECT_2);
        S3_WAL_OBJECT_METADATA_LIST.add(new S3WALObjectMetadata(walObject, objectMetadata));
    }

    @AfterEach
    public void tearDown() {
        S3_WAL_OBJECT_METADATA_LIST.clear();
    }

    private boolean compare(StreamDataBlock block1, StreamDataBlock block2) {
        return block1.getStreamId() == block2.getStreamId() &&
                block1.getStartOffset() == block2.getStartOffset() &&
                block1.getEndOffset() == block2.getEndOffset() &&
                block1.getRecordCount() == block2.getRecordCount() &&
                block1.getObjectId() == block2.getObjectId();
    }

    private boolean compare(List<StreamDataBlock> streamDataBlocks1, List<StreamDataBlock> streamDataBlocks2) {
        if (streamDataBlocks1.size() != streamDataBlocks2.size()) {
            return false;
        }
        for (int i = 0; i < streamDataBlocks1.size(); i++) {
            if (!compare(streamDataBlocks1.get(i), streamDataBlocks2.get(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean compare(CompactedObjectBuilder builder1, CompactedObjectBuilder builder2) {
        if (builder1.type() != builder2.type()) {
            return false;
        }
        return compare(builder1.streamDataBlocks(), builder2.streamDataBlocks());
    }

    private boolean compare(CompactedObject compactedObject1, CompactedObject compactedObject2) {
        if (compactedObject1.type() != compactedObject2.type()) {
            return false;
        }
        return compare(compactedObject1.streamDataBlocks(), compactedObject2.streamDataBlocks());
    }

    @Test
    public void testReadObjectIndices() {
        this.compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, EXECUTION_SCORE_THRESHOLD, STREAM_SPLIT_SIZE, s3Operator);
        List<StreamDataBlock> streamDataBlocks = this.compactionAnalyzer.blockWaitObjectIndices(S3_WAL_OBJECT_METADATA_LIST);
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
    public void testShouldCompact() {
        this.compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, EXECUTION_SCORE_THRESHOLD, STREAM_SPLIT_SIZE, s3Operator);
        List<StreamDataBlock> streamDataBlocks = this.compactionAnalyzer.blockWaitObjectIndices(S3_WAL_OBJECT_METADATA_LIST);
        Assertions.assertTrue(this.compactionAnalyzer.shouldCompact(streamDataBlocks));
    }

    @Test
    public void testSortStreamRangePositions() {
        this.compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, EXECUTION_SCORE_THRESHOLD, STREAM_SPLIT_SIZE, s3Operator);
        List<StreamDataBlock> streamDataBlocks = this.compactionAnalyzer.blockWaitObjectIndices(S3_WAL_OBJECT_METADATA_LIST);
        List<StreamDataBlock> sortedStreamDataBlocks = this.compactionAnalyzer.sortStreamRangePositions(streamDataBlocks);
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
        this.compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, EXECUTION_SCORE_THRESHOLD, 100, s3Operator);
        List<CompactedObjectBuilder> compactedObjectBuilders = this.compactionAnalyzer.buildCompactedObjects(S3_WAL_OBJECT_METADATA_LIST);
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
        this.compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, EXECUTION_SCORE_THRESHOLD, 30, s3Operator);
        List<CompactedObjectBuilder> compactedObjectBuilders = this.compactionAnalyzer.buildCompactedObjects(S3_WAL_OBJECT_METADATA_LIST);
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
        this.compactionAnalyzer = new CompactionAnalyzer(CACHE_SIZE, EXECUTION_SCORE_THRESHOLD, 100, s3Operator);
        List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(S3_WAL_OBJECT_METADATA_LIST);
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

    @Test
    public void testCompactionPlans2() {
        this.compactionAnalyzer = new CompactionAnalyzer(300, EXECUTION_SCORE_THRESHOLD, 100, s3Operator);
        List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(S3_WAL_OBJECT_METADATA_LIST);
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
    public void testCompactionPlanWithException() {

    }
}
