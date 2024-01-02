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
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.compact.objects.CompactedObject;
import com.automq.stream.s3.compact.objects.CompactionType;
import com.automq.stream.s3.compact.operator.DataBlockReader;
import com.automq.stream.s3.memory.MemoryMetadataManager;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.MemoryS3Operator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(30)
@Tag("S3Unit")
public class CompactionUploaderTest extends CompactionTestBase {

    private MemoryMetadataManager objectManager;
    private Config config;

    @BeforeEach
    public void setUp() throws Exception {
        s3Operator = new MemoryS3Operator();
        objectManager = new MemoryMetadataManager();
        config = mock(Config.class);
        when(config.networkBaselineBandwidth()).thenReturn(500L);
        when(config.streamSetObjectCompactionUploadConcurrency()).thenReturn(3);
        when(config.objectPartSize()).thenReturn(100);
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
        CompletableFuture<Void> cf = uploader.chainWriteStreamSetObject(null, compactedObject);
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            streamDataBlock.getDataCf().complete(TestUtils.random(streamDataBlock.getBlockSize()));
        }
        cf.thenAccept(v -> uploader.forceUploadStreamSetObject()).join();
        uploader.forceUploadStreamSetObject().join();
        long walObjectSize = uploader.complete();
        System.out.printf("write size: %d%n", walObjectSize);
        assertEquals(walObjectSize, calculateObjectSize(streamDataBlocks));

        //check s3 object
        DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(OBJECT_0, walObjectSize, S3ObjectType.STREAM_SET), s3Operator);
        reader.parseDataBlockIndex();
        List<StreamDataBlock> streamDataBlocksFromS3 = reader.getDataBlockIndex().join();
        assertEquals(streamDataBlocksFromS3.size(), streamDataBlocks.size());
        reader.readBlocks(streamDataBlocksFromS3);
        long expectedBlockPosition = 0;
        for (int i = 0; i < streamDataBlocks.size(); i++) {
            assertEquals(expectedBlockPosition, streamDataBlocksFromS3.get(i).getBlockStartPosition());
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
        CompletableFuture<Void> cf = uploader.chainWriteStreamSetObject(null, compactedObject);
        cf = uploader.chainWriteStreamSetObject(cf, compactedObject2);

        for (StreamDataBlock streamDataBlock : streamDataBlocks2) {
            streamDataBlock.getDataCf().complete(TestUtils.random(streamDataBlock.getBlockSize()));
        }

        for (StreamDataBlock streamDataBlock : streamDataBlocks1) {
            streamDataBlock.getDataCf().complete(TestUtils.random(streamDataBlock.getBlockSize()));
        }

        cf.thenAccept(v -> uploader.forceUploadStreamSetObject()).join();
        uploader.forceUploadStreamSetObject().join();
        long walObjectSize = uploader.complete();

        List<StreamDataBlock> expectedDataBlocks = new ArrayList<>(streamDataBlocks1);
        expectedDataBlocks.addAll(streamDataBlocks2);
        assertEquals(walObjectSize, calculateObjectSize(expectedDataBlocks));

        //check s3 object
        DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(OBJECT_0, walObjectSize, S3ObjectType.STREAM_SET), s3Operator);
        reader.parseDataBlockIndex();
        List<StreamDataBlock> streamDataBlocksFromS3 = reader.getDataBlockIndex().join();
        assertEquals(streamDataBlocksFromS3.size(), expectedDataBlocks.size());
        reader.readBlocks(streamDataBlocksFromS3);
        long expectedBlockPosition = 0;
        for (int i = 0; i < expectedDataBlocks.size(); i++) {
            assertEquals(expectedBlockPosition, streamDataBlocksFromS3.get(i).getBlockStartPosition());
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
        assertEquals(streamObject.getObjectSize(), calculateObjectSize(streamDataBlocks));

        //check s3 object
        DataBlockReader reader = new DataBlockReader(new S3ObjectMetadata(OBJECT_0, streamObject.getObjectSize(), S3ObjectType.STREAM), s3Operator);
        reader.parseDataBlockIndex();
        List<StreamDataBlock> streamDataBlocksFromS3 = reader.getDataBlockIndex().join();
        assertEquals(streamDataBlocksFromS3.size(), streamDataBlocks.size());
        reader.readBlocks(streamDataBlocksFromS3);
        long expectedBlockPosition = 0;
        for (int i = 0; i < streamDataBlocks.size(); i++) {
            assertEquals(expectedBlockPosition, streamDataBlocksFromS3.get(i).getBlockStartPosition());
            expectedBlockPosition += streamDataBlocksFromS3.get(i).getBlockSize();
            compare(streamDataBlocksFromS3.get(i), streamDataBlocks.get(i));
        }
    }
}
