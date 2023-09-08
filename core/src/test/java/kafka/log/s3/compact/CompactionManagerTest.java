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

import kafka.log.s3.compact.objects.CompactionType;
import kafka.log.s3.compact.objects.StreamDataBlock;
import kafka.log.s3.memory.MemoryMetadataManager;
import kafka.log.s3.metadata.StreamMetadataManager;
import kafka.log.s3.objects.CommitWALObjectRequest;
import kafka.log.s3.objects.StreamObject;
import kafka.log.s3.operator.MemoryS3Operator;
import kafka.server.KafkaConfig;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.metadata.stream.S3WALObjectMetadata;
import org.apache.kafka.metadata.stream.StreamOffsetRange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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
    public void testCompact() {
        List<S3WALObjectMetadata> s3ObjectMetadata = this.streamMetadataManager.getWALObjects();
        List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(s3ObjectMetadata);
        Assertions.assertEquals(2, compactionPlans.size());
        CommitWALObjectRequest request = compactionManager.buildCompactRequest(compactionPlans, s3ObjectMetadata);

        Assertions.assertEquals(List.of(OBJECT_0, OBJECT_1, OBJECT_2), request.getCompactedObjectIds());
        Assertions.assertEquals(OBJECT_0, request.getOrderId());
        Assertions.assertTrue(request.getObjectId() > OBJECT_2);
        request.getStreamObjects().forEach(s -> Assertions.assertTrue(s.getObjectId() > OBJECT_2));
        Assertions.assertEquals(3, request.getStreamObjects().size());
        Assertions.assertEquals(2, request.getStreamRanges().size());

        List<StreamDataBlock> walStreamDataBlocks = compactionPlans.stream()
                .map(p -> p.compactedObjects().stream()
                        .filter(c -> c.type() == CompactionType.COMPACT)
                        .flatMap(c -> c.streamDataBlocks().stream()).collect(Collectors.toList()))
                .flatMap(List::stream).collect(Collectors.toList());
        long expectedWALSize = calculateObjectSize(walStreamDataBlocks);
        Assertions.assertEquals(expectedWALSize, request.getObjectSize());
    }

    @Test
    public void testCompactNoneExistObjects() {
        List<S3WALObjectMetadata> s3ObjectMetadata = this.streamMetadataManager.getWALObjects();
        List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(s3ObjectMetadata);
        s3Operator.delete(s3ObjectMetadata.get(0).getObjectMetadata().key()).join();

        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> compactionManager.buildCompactRequest(compactionPlans, s3ObjectMetadata));
    }
}
