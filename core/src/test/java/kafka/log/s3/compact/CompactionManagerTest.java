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

import kafka.log.s3.compact.objects.StreamDataBlock;
import kafka.log.s3.compact.operator.DataBlockReader;
import kafka.log.s3.objects.CommitWALObjectRequest;
import kafka.log.s3.objects.ObjectStreamRange;
import kafka.log.s3.objects.StreamObject;
import kafka.server.KafkaConfig;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.apache.kafka.metadata.stream.S3StreamConstant;
import org.apache.kafka.metadata.stream.StreamOffsetRange;
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

@Timeout(60)
@Tag("S3Unit")
public class CompactionManagerTest extends CompactionTestBase {
    private static final int BROKER0 = 0;
    private CompactionAnalyzer compactionAnalyzer;
    private KafkaConfig config;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        config = Mockito.mock(KafkaConfig.class);
        Mockito.when(config.brokerId()).thenReturn(BROKER0);
        Mockito.when(config.s3ObjectCompactionNWInBandwidth()).thenReturn(500L);
        Mockito.when(config.s3ObjectCompactionNWOutBandwidth()).thenReturn(500L);
        Mockito.when(config.s3ObjectCompactionUploadConcurrency()).thenReturn(3);
        Mockito.when(config.s3ObjectPartSize()).thenReturn(100);
        Mockito.when(config.s3ObjectCompactionCacheSize()).thenReturn(300L);
        Mockito.when(config.s3ObjectCompactionExecutionScoreThreshold()).thenReturn(0.5);
        Mockito.when(config.s3ObjectCompactionStreamSplitSize()).thenReturn(100L);
        Mockito.when(config.s3ObjectCompactionForceSplitPeriod()).thenReturn(120);
        Mockito.when(config.s3ObjectCompactionMaxObjectNum()).thenReturn(100);
        compactionAnalyzer = new CompactionAnalyzer(config.s3ObjectCompactionCacheSize(),
                config.s3ObjectCompactionExecutionScoreThreshold(), config.s3ObjectCompactionStreamSplitSize(), s3Operator);
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testFilterS3Object() {
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        Mockito.when(config.s3ObjectCompactionMaxObjectNum()).thenReturn(2);
        CompactionManager compactionManager = new CompactionManager(config, objectManager, s3Operator);
        Map<Boolean, List<S3ObjectMetadata>> filteredMap = compactionManager.filterS3Objects(s3ObjectMetadata);
        for (List<S3ObjectMetadata> list : filteredMap.values()) {
            Assertions.assertTrue(list.size() <= 2);
        }
    }

    @Test
    public void testForceSplit() {
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        List<StreamDataBlock> streamDataBlocks = CompactionUtils.blockWaitObjectIndices(s3ObjectMetadata, s3Operator)
                .values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        CompactionManager compactionManager = new CompactionManager(config, objectManager, s3Operator);
        List<CompletableFuture<StreamObject>> cfList = compactionManager.splitWALObjects(s3ObjectMetadata);
        List<StreamObject> streamObjects = cfList.stream().map(CompletableFuture::join).collect(Collectors.toList());

        for (StreamObject streamObject : streamObjects) {
            StreamDataBlock streamDataBlock = get(streamDataBlocks, streamObject);
            Assertions.assertNotNull(streamDataBlock);
            Assertions.assertEquals(calculateObjectSize(List.of(streamDataBlock)), streamObject.getObjectSize());

            DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(streamObject.getObjectId(),
                    streamObject.getObjectSize(), S3ObjectType.STREAM), s3Operator);
            reader.parseDataBlockIndex();
            List<StreamDataBlock> streamDataBlocksFromS3 = reader.getDataBlockIndex().join();
            Assertions.assertEquals(1, streamDataBlocksFromS3.size());
            Assertions.assertEquals(0, streamDataBlocksFromS3.get(0).getBlockStartPosition());
            Assertions.assertTrue(contains(streamDataBlocks, streamDataBlocksFromS3.get(0)));
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
        CompactionManager compactionManager = new CompactionManager(config, objectManager, s3Operator);
        CommitWALObjectRequest request = compactionManager.buildCompactRequest(s3ObjectMetadata);

        Assertions.assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        Assertions.assertEquals(OBJECT_0, request.getOrderId());
        Assertions.assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> Assertions.assertTrue(s.getObjectId() > OBJECT_2));
        Assertions.assertEquals(3, request.getStreamObjects().size());
        Assertions.assertEquals(2, request.getStreamRanges().size());
    }

    @Test
    public void testCompactNoneExistObjects() {
        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();
        CompactionManager compactionManager = new CompactionManager(config, objectManager, s3Operator);
        List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(s3ObjectMetadata);
        s3Operator.delete(s3ObjectMetadata.get(0).key()).join();
        CommitWALObjectRequest request = new CommitWALObjectRequest();
        Assertions.assertThrowsExactly(IllegalArgumentException.class,
                () -> compactionManager.compactWALObjects(request, compactionPlans, s3ObjectMetadata));
    }

    private void testCompactWithNWInThrottle(long networkInThreshold) {
        Mockito.when(config.s3ObjectCompactionNWInBandwidth()).thenReturn(networkInThreshold);
        CompactionManager compactionManager = new CompactionManager(config, objectManager, s3Operator);

        List<S3ObjectMetadata> s3ObjectMetadata = this.objectManager.getServerObjects().join();

        long expectedMinCompleteTime = 530 / networkInThreshold;
        System.out.printf("Expect to complete no less than %ds%n", expectedMinCompleteTime);
        long start = System.currentTimeMillis();
        CommitWALObjectRequest request = compactionManager.buildCompactRequest(s3ObjectMetadata);
        long cost = System.currentTimeMillis() - start;
        System.out.printf("Cost %ds%n", cost / 1000);
        Assertions.assertTrue(cost > expectedMinCompleteTime * 1000);

        Assertions.assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        Assertions.assertEquals(OBJECT_0, request.getOrderId());
        Assertions.assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> Assertions.assertTrue(s.getObjectId() > OBJECT_2));
        Assertions.assertEquals(3, request.getStreamObjects().size());
        Assertions.assertEquals(2, request.getStreamRanges().size());

        // check data integrity
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
        Assertions.assertEquals(expectedStreamDataBlocks.size(), compactedStreamDataBlocks.size());
        for (StreamDataBlock compactedStreamDataBlock : compactedStreamDataBlocks) {
            Assertions.assertTrue(expectedStreamDataBlocks.stream().anyMatch(s -> compare(compactedStreamDataBlock, s)),
                    "broken block: " + compactedStreamDataBlock);
        }
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
