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

import java.util.ArrayList;
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
                new StreamDataBlock(STREAM_0, 0, 20, 2, 1, 30, 20, 1),
                new StreamDataBlock(STREAM_0, 20, 25, 3, 0, 10, 5, 1),
                new StreamDataBlock(STREAM_2, 40, 120, 0, 2, 100, 80, 1),
                new StreamDataBlock(STREAM_2, 120, 150, 1, 3, 0, 30, 1));
        CompactedObject compactedObject = new CompactedObject(CompactionType.COMPACT, streamDataBlocks);
        CompactionUploader uploader = new CompactionUploader(objectManager, s3Operator, config);
        CompletableFuture<Void> cf = uploader.chainWriteWALObject(null, compactedObject);
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            streamDataBlock.getDataCf().complete(TestUtils.random(streamDataBlock.getBlockSize()));
        }
        cf.thenAccept(v -> uploader.forceUploadWAL()).join();
        uploader.forceUploadWAL().join();
        long walObjectSize = uploader.completeWAL();
        System.out.printf("write size: %d%n", walObjectSize);
        Assertions.assertEquals(walObjectSize, calculateObjectSize(streamDataBlocks));

        //check s3 object
        DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(OBJECT_0, walObjectSize, S3ObjectType.WAL), s3Operator);
        reader.parseDataBlockIndex();
        List<StreamDataBlock> streamDataBlocksFromS3 = reader.getDataBlockIndex().join();
        Assertions.assertEquals(streamDataBlocksFromS3.size(), streamDataBlocks.size());
        reader.readBlocks(streamDataBlocksFromS3);
        long expectedBlockPosition = 0;
        for (int i = 0; i < streamDataBlocks.size(); i++) {
            Assertions.assertEquals(expectedBlockPosition, streamDataBlocksFromS3.get(i).getBlockStartPosition());
            expectedBlockPosition += streamDataBlocksFromS3.get(i).getBlockSize();
            compare(streamDataBlocksFromS3.get(i), streamDataBlocks.get(i));
        }
    }

    @Test
    public void testWriteWALObject2() {
        List<StreamDataBlock> streamDataBlocks1 = List.of(
                new StreamDataBlock(STREAM_0, 0, 20, 2, 1, 30, 20, 1),
                new StreamDataBlock(STREAM_0, 20, 25, 3, 0, 10, 5, 1),
                new StreamDataBlock(STREAM_2, 40, 120, 0, 2, 100, 80, 1),
                new StreamDataBlock(STREAM_2, 120, 150, 1, 3, 0, 30, 1));
        CompactedObject compactedObject = new CompactedObject(CompactionType.COMPACT, streamDataBlocks1);

        List<StreamDataBlock> streamDataBlocks2 = List.of(
                new StreamDataBlock(STREAM_3, 0, 15, 2, 4, 0, 15, 1),
                new StreamDataBlock(STREAM_3, 15, 20, 1, 5, 20, 5, 1));
        CompactedObject compactedObject2 = new CompactedObject(CompactionType.COMPACT, streamDataBlocks2);

        CompactionUploader uploader = new CompactionUploader(objectManager, s3Operator, config);
        CompletableFuture<Void> cf = uploader.chainWriteWALObject(null, compactedObject);
        cf = uploader.chainWriteWALObject(cf, compactedObject2);

        for (StreamDataBlock streamDataBlock : streamDataBlocks2) {
            streamDataBlock.getDataCf().complete(TestUtils.random(streamDataBlock.getBlockSize()));
        }

        for (StreamDataBlock streamDataBlock : streamDataBlocks1) {
            streamDataBlock.getDataCf().complete(TestUtils.random(streamDataBlock.getBlockSize()));
        }

        cf.thenAccept(v -> uploader.forceUploadWAL()).join();
        uploader.forceUploadWAL().join();
        long walObjectSize = uploader.completeWAL();

        List<StreamDataBlock> expectedDataBlocks = new ArrayList<>(streamDataBlocks1);
        expectedDataBlocks.addAll(streamDataBlocks2);
        Assertions.assertEquals(walObjectSize, calculateObjectSize(expectedDataBlocks));

        //check s3 object
        DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(OBJECT_0, walObjectSize, S3ObjectType.WAL), s3Operator);
        reader.parseDataBlockIndex();
        List<StreamDataBlock> streamDataBlocksFromS3 = reader.getDataBlockIndex().join();
        Assertions.assertEquals(streamDataBlocksFromS3.size(), expectedDataBlocks.size());
        reader.readBlocks(streamDataBlocksFromS3);
        long expectedBlockPosition = 0;
        for (int i = 0; i < expectedDataBlocks.size(); i++) {
            Assertions.assertEquals(expectedBlockPosition, streamDataBlocksFromS3.get(i).getBlockStartPosition());
            expectedBlockPosition += streamDataBlocksFromS3.get(i).getBlockSize();
            compare(streamDataBlocksFromS3.get(i), expectedDataBlocks.get(i));
        }
    }

    @Test
    public void testWriteStreamObject() {
        List<StreamDataBlock> streamDataBlocks = List.of(
                new StreamDataBlock(STREAM_0, 0, 60, 1, 0, 23, 60, 1),
                new StreamDataBlock(STREAM_0, 60, 120, 0, 1, 45, 60, 1));
        CompactedObject compactedObject = new CompactedObject(CompactionType.SPLIT, streamDataBlocks);

        CompactionUploader uploader = new CompactionUploader(objectManager, s3Operator, config);
        CompletableFuture<StreamObject> cf = uploader.writeStreamObject(compactedObject);
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            streamDataBlock.getDataCf().complete(TestUtils.random((int) streamDataBlock.getStreamRangeSize()));
        }
        StreamObject streamObject = cf.join();
        Assertions.assertEquals(streamObject.getObjectSize(), calculateObjectSize(streamDataBlocks));

        //check s3 object
        DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(OBJECT_0, streamObject.getObjectSize(), S3ObjectType.STREAM), s3Operator);
        reader.parseDataBlockIndex();
        List<StreamDataBlock> streamDataBlocksFromS3 = reader.getDataBlockIndex().join();
        Assertions.assertEquals(streamDataBlocksFromS3.size(), streamDataBlocks.size());
        reader.readBlocks(streamDataBlocksFromS3);
        long expectedBlockPosition = 0;
        for (int i = 0; i < streamDataBlocks.size(); i++) {
            Assertions.assertEquals(expectedBlockPosition, streamDataBlocksFromS3.get(i).getBlockStartPosition());
            expectedBlockPosition += streamDataBlocksFromS3.get(i).getBlockSize();
            compare(streamDataBlocksFromS3.get(i), streamDataBlocks.get(i));
        }
    }
}
