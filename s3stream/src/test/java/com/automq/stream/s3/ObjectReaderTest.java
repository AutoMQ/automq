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

package com.automq.stream.s3;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.MemoryS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class ObjectReaderTest {

    @Test
    public void testIndexBlock() {
        // block0: s1 [0, 100)
        // block1: s1 [100, 300)
        // block2: s1 [300, 400)
        // block3: s2 [110, 200)
        ByteBuf blocks = Unpooled.buffer(3 * ObjectReader.DataBlockIndex.BLOCK_INDEX_SIZE);
        blocks.writeLong(0);
        blocks.writeInt(1024);
        blocks.writeInt(100);

        blocks.writeLong(1024);
        blocks.writeInt(512);
        blocks.writeInt(100);

        blocks.writeLong(1536);
        blocks.writeInt(512);
        blocks.writeInt(100);

        blocks.writeLong(2048);
        blocks.writeInt(512);
        blocks.writeInt(90);


        ByteBuf streamRanges = Unpooled.buffer(3 * (8 + 8 + 4 + 4));
        streamRanges.writeLong(1);
        streamRanges.writeLong(0);
        streamRanges.writeInt(100);
        streamRanges.writeInt(0);

        streamRanges.writeLong(1);
        streamRanges.writeLong(100);
        streamRanges.writeInt(200);
        streamRanges.writeInt(1);

        streamRanges.writeLong(1);
        streamRanges.writeLong(300);
        streamRanges.writeInt(400);
        streamRanges.writeInt(2);

        streamRanges.writeLong(2);
        streamRanges.writeLong(110);
        streamRanges.writeInt(90);
        streamRanges.writeInt(2);

        ObjectReader.IndexBlock indexBlock = new ObjectReader.IndexBlock(blocks, streamRanges);

        ObjectReader.FindIndexResult rst = indexBlock.find(1, 10, 300, 100000);
        assertTrue(rst.isFulfilled());
        List<StreamDataBlock> streamDataBlocks = rst.streamDataBlocks();
        assertEquals(2, streamDataBlocks.size());
        assertEquals(0, streamDataBlocks.get(0).getBlockId());
        assertEquals(0, streamDataBlocks.get(0).getBlockStartPosition());
        assertEquals(1024, streamDataBlocks.get(0).getBlockEndPosition());
        assertEquals(1, streamDataBlocks.get(1).getBlockId());
        assertEquals(1024, streamDataBlocks.get(1).getBlockStartPosition());
        assertEquals(1536, streamDataBlocks.get(1).getBlockEndPosition());

        rst = indexBlock.find(1, 10, 400);
        assertTrue(rst.isFulfilled());
        assertEquals(3, rst.streamDataBlocks().size());

        rst = indexBlock.find(1, 10, 400, 10);
        assertTrue(rst.isFulfilled());
        assertEquals(2, rst.streamDataBlocks().size());

        rst = indexBlock.find(1, 10, 800);
        assertFalse(rst.isFulfilled());
        assertEquals(3, rst.streamDataBlocks().size());
    }

    @Test
    public void testGetBasicObjectInfo() throws ExecutionException, InterruptedException {
        S3Operator s3Operator = new MemoryS3Operator();
        ObjectWriter objectWriter = ObjectWriter.writer(233L, s3Operator, 1024, 1024);
        // make index block bigger than 1M
        int streamCount = 2 * 1024 * 1024 / 40;
        for (int i = 0; i < streamCount; i++) {
            StreamRecordBatch r = new StreamRecordBatch(i, 0, i, 1, TestUtils.random(1));
            objectWriter.write(i, List.of(r));
        }
        objectWriter.close().get();
        S3ObjectMetadata metadata = new S3ObjectMetadata(233L, objectWriter.size(), S3ObjectType.STREAM_SET);
        try (ObjectReader objectReader = new ObjectReader(metadata, s3Operator)) {
            ObjectReader.BasicObjectInfo info = objectReader.basicObjectInfo().get();
            assertEquals(streamCount, info.blockCount());
        }
    }

}
