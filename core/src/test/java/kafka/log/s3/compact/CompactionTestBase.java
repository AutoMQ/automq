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
import kafka.log.s3.compact.objects.StreamDataBlock;
import kafka.log.s3.memory.MemoryMetadataManager;
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.operator.MemoryS3Operator;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.apache.kafka.metadata.stream.StreamOffsetRange;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class CompactionTestBase {
    protected static final int BROKER_0 = 0;
    protected static final long STREAM_0 = 0;
    protected static final long STREAM_1 = 1;
    protected static final long STREAM_2 = 2;
    protected static final long STREAM_3 = 3;
    protected static final long OBJECT_0 = 0;
    protected static final long OBJECT_1 = 1;
    protected static final long OBJECT_2 = 2;
    protected static final long OBJECT_3 = 3;
    protected static final long CACHE_SIZE = 1024;
    protected static final double EXECUTION_SCORE_THRESHOLD = 0.5;
    protected static final long STREAM_SPLIT_SIZE = 30;
    protected static final List<S3ObjectMetadata> S3_WAL_OBJECT_METADATA_LIST = new ArrayList<>();
    protected MemoryMetadataManager objectManager;
    protected S3Operator s3Operator;

    public void setUp() throws Exception {
        objectManager = Mockito.spy(MemoryMetadataManager.class);
        objectManager.start();
        s3Operator = new MemoryS3Operator();
        // stream data for object 0
        objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30)).thenAccept(objectId -> {
            Assertions.assertEquals(OBJECT_0, objectId);
            ObjectWriter objectWriter = ObjectWriter.writer(objectId, s3Operator, 1024, 1024);
            StreamRecordBatch r1 = new StreamRecordBatch(STREAM_0, 0, 0, 20, TestUtils.random(20));
            StreamRecordBatch r2 = new StreamRecordBatch(STREAM_1, 0, 30, 30, TestUtils.random(30));
            StreamRecordBatch r3 = new StreamRecordBatch(STREAM_2, 0, 30, 30, TestUtils.random(30));
            objectWriter.write(STREAM_0, List.of(r1));
            objectWriter.write(STREAM_1, List.of(r2));
            objectWriter.write(STREAM_2, List.of(r3));
            objectWriter.close().join();
            List<StreamOffsetRange> streamsIndices = List.of(
                    new StreamOffsetRange(STREAM_0, 0, 20),
                    new StreamOffsetRange(STREAM_1, 30, 60),
                    new StreamOffsetRange(STREAM_2, 30, 60)
            );
            S3ObjectMetadata objectMetadata = new S3ObjectMetadata(OBJECT_0, S3ObjectType.WAL, streamsIndices, System.currentTimeMillis(),
                    System.currentTimeMillis(), objectWriter.size(), OBJECT_0);
            S3_WAL_OBJECT_METADATA_LIST.add(objectMetadata);
        }).join();

        // stream data for object 1
        objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30)).thenAccept(objectId -> {
            Assertions.assertEquals(OBJECT_1, objectId);
            ObjectWriter objectWriter = ObjectWriter.writer(OBJECT_1, s3Operator, 1024, 1024);
            StreamRecordBatch r4 = new StreamRecordBatch(STREAM_0, 0, 20, 5, TestUtils.random(5));
            StreamRecordBatch r5 = new StreamRecordBatch(STREAM_1, 0, 60, 60, TestUtils.random(60));
            objectWriter.write(STREAM_0, List.of(r4));
            objectWriter.write(STREAM_1, List.of(r5));
            objectWriter.close().join();
            List<StreamOffsetRange> streamsIndices = List.of(
                    new StreamOffsetRange(STREAM_0, 20, 25),
                    new StreamOffsetRange(STREAM_1, 60, 120)
            );
            S3ObjectMetadata objectMetadata = new S3ObjectMetadata(OBJECT_1, S3ObjectType.WAL, streamsIndices, System.currentTimeMillis(),
                    System.currentTimeMillis(), objectWriter.size(), OBJECT_1);
            S3_WAL_OBJECT_METADATA_LIST.add(objectMetadata);
        }).join();

        // stream data for object 2
        objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30)).thenAccept(objectId -> {
            Assertions.assertEquals(OBJECT_2, objectId);
            ObjectWriter objectWriter = ObjectWriter.writer(OBJECT_2, s3Operator, 1024, 1024);
            // redundant record
            StreamRecordBatch r6 = new StreamRecordBatch(STREAM_1, 0, 260, 20, TestUtils.random(20));
            StreamRecordBatch r7 = new StreamRecordBatch(STREAM_1, 0, 400, 100, TestUtils.random(100));
            StreamRecordBatch r8 = new StreamRecordBatch(STREAM_2, 0, 230, 40, TestUtils.random(40));
            objectWriter.write(STREAM_1, List.of(r6));
            objectWriter.write(STREAM_1, List.of(r7));
            objectWriter.write(STREAM_2, List.of(r8));
            objectWriter.close().join();
            List<StreamOffsetRange> streamsIndices = List.of(
                    new StreamOffsetRange(STREAM_1, 400, 500),
                    new StreamOffsetRange(STREAM_2, 230, 270)
            );
            S3ObjectMetadata objectMetadata = new S3ObjectMetadata(OBJECT_2, S3ObjectType.WAL, streamsIndices, System.currentTimeMillis(),
                    System.currentTimeMillis(), objectWriter.size(), OBJECT_2);
            S3_WAL_OBJECT_METADATA_LIST.add(objectMetadata);
        }).join();
        Mockito.doReturn(CompletableFuture.completedFuture(S3_WAL_OBJECT_METADATA_LIST)).when(objectManager).getServerObjects();
    }

    public void tearDown() {
        S3_WAL_OBJECT_METADATA_LIST.clear();
        objectManager.shutdown();
    }

    protected boolean compare(StreamDataBlock block1, StreamDataBlock block2) {
        boolean attr = block1.getStreamId() == block2.getStreamId() &&
                block1.getStartOffset() == block2.getStartOffset() &&
                block1.getEndOffset() == block2.getEndOffset() &&
                block1.getRecordCount() == block2.getRecordCount();
        if (!attr) {
            return false;
        }
        if (!block1.getDataCf().isDone()) {
            return !block2.getDataCf().isDone();
        } else {
            if (!block2.getDataCf().isDone()) {
                return false;
            } else {
                return block1.getDataCf().join().compareTo(block2.getDataCf().join()) == 0;
            }
        }
    }

    protected boolean compare(List<StreamDataBlock> streamDataBlocks1, List<StreamDataBlock> streamDataBlocks2) {
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

    protected boolean compare(Map<Long, List<StreamDataBlock>> streamDataBlockMap1, Map<Long, List<StreamDataBlock>> streamDataBlockMap2) {
        if (streamDataBlockMap1.size() != streamDataBlockMap2.size()) {
            return false;
        }
        for (Map.Entry<Long, List<StreamDataBlock>> entry : streamDataBlockMap1.entrySet()) {
            long objectId = entry.getKey();
            List<StreamDataBlock> streamDataBlocks = entry.getValue();
            Assertions.assertTrue(streamDataBlockMap2.containsKey(objectId));
            if (!compare(streamDataBlocks, streamDataBlockMap2.get(objectId))) {
                return false;
            }
        }
        return true;
    }

    protected boolean compare(CompactedObjectBuilder builder1, CompactedObjectBuilder builder2) {
        if (builder1.type() != builder2.type()) {
            return false;
        }
        return compare(builder1.streamDataBlocks(), builder2.streamDataBlocks());
    }

    protected boolean compare(CompactedObject compactedObject1, CompactedObject compactedObject2) {
        if (compactedObject1.type() != compactedObject2.type()) {
            return false;
        }
        return compare(compactedObject1.streamDataBlocks(), compactedObject2.streamDataBlocks());
    }

    protected long calculateObjectSize(List<StreamDataBlock> streamDataBlocks) {
        long bodySize = streamDataBlocks.stream().mapToLong(StreamDataBlock::getBlockSize).sum();
        long indexBlockSize = 4 + 40L * streamDataBlocks.size();
        long tailSize = 48;
        return bodySize + indexBlockSize + tailSize;
    }
}
