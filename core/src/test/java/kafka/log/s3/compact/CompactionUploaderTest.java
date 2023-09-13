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

import kafka.log.s3.TestUtils;
import kafka.log.s3.compact.objects.CompactedObject;
import kafka.log.s3.compact.objects.CompactionType;
import kafka.log.s3.compact.objects.StreamDataBlock;
import kafka.log.s3.compact.operator.DataBlockReader;
import kafka.log.s3.memory.MemoryMetadataManager;
import kafka.log.s3.objects.ObjectStreamRange;
import kafka.log.s3.objects.StreamObject;
import kafka.log.s3.operator.MemoryS3Operator;
import kafka.server.KafkaConfig;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Timeout(30)
@Tag("S3Unit")
public class CompactionUploaderTest extends CompactionTestBase {

    private MemoryMetadataManager objectManager;
    private KafkaConfig config;

    @BeforeEach
    public void setUp() throws Exception {
        s3Operator = new MemoryS3Operator();
        objectManager = new MemoryMetadataManager();
        objectManager.start();
        config = Mockito.mock(KafkaConfig.class);
        Mockito.when(config.s3ObjectCompactionNWOutBandwidth()).thenReturn(500L);
        Mockito.when(config.s3ObjectCompactionUploadConcurrency()).thenReturn(3);
        Mockito.when(config.s3ObjectPartSize()).thenReturn(100);
    }

    @AfterEach
    public void tearDown() {
        objectManager.shutdown();
    }

    @Test
    public void testWriteWALObject() {
        List<StreamDataBlock> streamDataBlocks = List.of(
                new StreamDataBlock(STREAM_0, 0, 20, 2, 1, 0, 20, 1),
                new StreamDataBlock(STREAM_0, 20, 25, 3, 0, 20, 5, 1),
                new StreamDataBlock(STREAM_2, 40, 120, 0, 2, 25, 80, 1),
                new StreamDataBlock(STREAM_2, 120, 150, 1, 3, 105, 30, 1));
        CompactedObject compactedObject = new CompactedObject(CompactionType.COMPACT, streamDataBlocks, 100);
        List<ObjectStreamRange> result = CompactionUtils.buildObjectStreamRange(compactedObject);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(STREAM_0, result.get(0).getStreamId());
        Assertions.assertEquals(0, result.get(0).getStartOffset());
        Assertions.assertEquals(25, result.get(0).getEndOffset());
        Assertions.assertEquals(STREAM_2, result.get(1).getStreamId());
        Assertions.assertEquals(40, result.get(1).getStartOffset());
        Assertions.assertEquals(150, result.get(1).getEndOffset());

        CompactionUploader uploader = new CompactionUploader(objectManager, s3Operator, config);
        CompletableFuture<CompletableFuture<Void>> cf = uploader.writeWALObject(compactedObject);
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            streamDataBlock.getDataCf().complete(TestUtils.random((int) streamDataBlock.getStreamRangeSize()));
        }
        CompletableFuture<Void> writeCf = cf.join();
        uploader.forceUploadWAL();
        writeCf.join();
        long walObjectSize = uploader.completeWAL();
        System.out.printf("write size: %d%n", walObjectSize);
        Assertions.assertEquals(walObjectSize, calculateObjectSize(streamDataBlocks));

        //check s3 object
        DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(OBJECT_0, walObjectSize, S3ObjectType.WAL), s3Operator);
        reader.parseDataBlockIndex();
        List<StreamDataBlock> streamDataBlocksFromS3 = reader.getDataBlockIndex().join();
        Assertions.assertEquals(streamDataBlocksFromS3.size(), streamDataBlocks.size());
        long expectedBlockPosition = 0;
        for (int i = 0; i < streamDataBlocks.size(); i++) {
            Assertions.assertEquals(expectedBlockPosition, streamDataBlocksFromS3.get(i).getBlockPosition());
            expectedBlockPosition += streamDataBlocksFromS3.get(i).getBlockSize();
            compare(streamDataBlocksFromS3.get(i), streamDataBlocks.get(i));
        }
        List<DataBlockReader.DataBlockIndex> blockIndices = CompactionUtils.buildBlockIndicesFromStreamDataBlock(streamDataBlocksFromS3);
        List<DataBlockReader.DataBlock> dataBlocks = reader.readBlocks(blockIndices).join();
        for (int i = 0; i < dataBlocks.size(); i++) {
            Assertions.assertEquals(streamDataBlocks.get(i).getDataCf().join(), dataBlocks.get(i).buffer());
        }
    }

    @Test
    public void testWriteStreamObject() {
        List<StreamDataBlock> streamDataBlocks = List.of(
                new StreamDataBlock(STREAM_0, 0, 60, 1, 0, 23, 60, 1),
                new StreamDataBlock(STREAM_0, 60, 120, 0, 1, 45, 60, 1));
        CompactedObject compactedObject = new CompactedObject(CompactionType.SPLIT, streamDataBlocks, 100);

        CompactionUploader uploader = new CompactionUploader(objectManager, s3Operator, config);
        CompletableFuture<StreamObject> cf = uploader.writeStreamObject(compactedObject);
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            streamDataBlock.getDataCf().complete(TestUtils.random((int) streamDataBlock.getStreamRangeSize()));
        }
        StreamObject streamObject = cf.join();
        System.out.printf("write size: %d%n", streamObject.getObjectSize());
        Assertions.assertEquals(streamObject.getObjectSize(), calculateObjectSize(streamDataBlocks));

        //check s3 object
        DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(OBJECT_0, streamObject.getObjectSize(), S3ObjectType.STREAM), s3Operator);
        reader.parseDataBlockIndex();
        List<StreamDataBlock> streamDataBlocksFromS3 = reader.getDataBlockIndex().join();
        Assertions.assertEquals(streamDataBlocksFromS3.size(), streamDataBlocks.size());
        long expectedBlockPosition = 0;
        for (int i = 0; i < streamDataBlocks.size(); i++) {
            Assertions.assertEquals(expectedBlockPosition, streamDataBlocksFromS3.get(i).getBlockPosition());
            expectedBlockPosition += streamDataBlocksFromS3.get(i).getBlockSize();
            compare(streamDataBlocksFromS3.get(i), streamDataBlocks.get(i));
        }
        List<DataBlockReader.DataBlockIndex> blockIndices = CompactionUtils.buildBlockIndicesFromStreamDataBlock(streamDataBlocksFromS3);
        List<DataBlockReader.DataBlock> dataBlocks = reader.readBlocks(blockIndices).join();
        for (int i = 0; i < dataBlocks.size(); i++) {
            Assertions.assertEquals(streamDataBlocks.get(i).getDataCf().join(), dataBlocks.get(i).buffer());
        }
    }
}
