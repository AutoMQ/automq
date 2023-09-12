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
import kafka.log.s3.metadata.StreamMetadataManager;
import kafka.log.s3.objects.CommitWALObjectRequest;
import kafka.log.s3.objects.StreamObject;
import kafka.server.KafkaConfig;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.apache.kafka.metadata.stream.S3WALObjectMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Timeout(30)
@Tag("S3Unit")
public class CompactionManagerTest extends CompactionTestBase {
    private static final int BROKER0 = 0;
    private StreamMetadataManager streamMetadataManager;
    private CompactionAnalyzer compactionAnalyzer;
    private CompactionManager compactionManager;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        KafkaConfig config = Mockito.mock(KafkaConfig.class);
        Mockito.when(config.brokerId()).thenReturn(BROKER0);
        Mockito.when(config.s3ObjectCompactionNWInBandwidth()).thenReturn(500L);
        Mockito.when(config.s3ObjectCompactionNWOutBandwidth()).thenReturn(500L);
        Mockito.when(config.s3ObjectCompactionUploadConcurrency()).thenReturn(3);
        Mockito.when(config.s3ObjectPartSize()).thenReturn(100);
        Mockito.when(config.s3ObjectCompactionCacheSize()).thenReturn(300L);
        Mockito.when(config.s3ObjectCompactionExecutionScoreThreshold()).thenReturn(0.5);
        Mockito.when(config.s3ObjectCompactionStreamSplitSize()).thenReturn(100L);
        Mockito.when(config.s3ObjectCompactionForceSplitPeriod()).thenReturn(120);
        streamMetadataManager = Mockito.mock(StreamMetadataManager.class);
        Mockito.when(streamMetadataManager.getWALObjects()).thenReturn(S3_WAL_OBJECT_METADATA_LIST);
        compactionAnalyzer = new CompactionAnalyzer(config.s3ObjectCompactionCacheSize(),
                config.s3ObjectCompactionExecutionScoreThreshold(), config.s3ObjectCompactionStreamSplitSize(), s3Operator);
        compactionManager = new CompactionManager(config, objectManager, streamMetadataManager, s3Operator);
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testForceSplit() {
        List<S3WALObjectMetadata> s3ObjectMetadata = this.streamMetadataManager.getWALObjects();
        List<StreamDataBlock> streamDataBlocks = CompactionUtils.blockWaitObjectIndices(s3ObjectMetadata, s3Operator)
                .values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        List<CompletableFuture<StreamObject>> cfList = this.compactionManager.splitWALObjects(s3ObjectMetadata);
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
            Assertions.assertEquals(0, streamDataBlocksFromS3.get(0).getBlockPosition());
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
        List<S3WALObjectMetadata> s3ObjectMetadata = this.streamMetadataManager.getWALObjects();
        CommitWALObjectRequest request = this.compactionManager.buildCompactRequest(s3ObjectMetadata);

        Assertions.assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        Assertions.assertEquals(OBJECT_0, request.getOrderId());
        Assertions.assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> Assertions.assertTrue(s.getObjectId() > OBJECT_2));
        Assertions.assertEquals(3, request.getStreamObjects().size());
        Assertions.assertEquals(2, request.getStreamRanges().size());
    }

    @Test
    public void testCompactNoneExistObjects() {
        List<S3WALObjectMetadata> s3ObjectMetadata = this.streamMetadataManager.getWALObjects();
        List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(s3ObjectMetadata);
        s3Operator.delete(s3ObjectMetadata.get(0).getObjectMetadata().key()).join();
        CommitWALObjectRequest request = new CommitWALObjectRequest();
        Assertions.assertThrowsExactly(IllegalArgumentException.class,
                () -> compactionManager.compactWALObjects(request, compactionPlans, s3ObjectMetadata));
    }
}
