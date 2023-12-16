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
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class ObjectReaderTest {

    private int recordCntToBlockSize(int recordCnt, int bodySize) {
        return (bodySize + StreamRecordBatchCodec.HEADER_SIZE) * recordCnt + ObjectWriter.DataBlock.BLOCK_HEADER_SIZE;
    }

    @Test
    public void testIndexBlock() {
        // block0: s1 [0, 100)
        // block1: s1 [100, 150)
        // block2: s1 [150, 200)
        // block3: s2 [110, 200)
        int bodySize = 10;
        int recordCnt1 = 100;
        int blockSize1 = recordCntToBlockSize(recordCnt1, bodySize);
        int recordCnt2 = 50;
        int blockSize2 = recordCntToBlockSize(recordCnt2, bodySize);
        int recordCnt3 = 90;
        int blockSize3 = recordCntToBlockSize(recordCnt3, bodySize);
        long streamId1 = 1;
        long streamId2 = 2;
        ByteBuf blocks = Unpooled.buffer(3 * ObjectReader.DataBlockIndex.BLOCK_INDEX_SIZE);
        blocks.writeLong(0);
        blocks.writeInt(blockSize1);
        blocks.writeInt(recordCnt1);

        blocks.writeLong(blockSize1);
        blocks.writeInt(blockSize2);
        blocks.writeInt(recordCnt2);

        blocks.writeLong((long) blockSize1 + blockSize2);
        blocks.writeInt(blockSize2);
        blocks.writeInt(recordCnt2);

        blocks.writeLong((long) blockSize1 + blockSize2 + blockSize2);
        blocks.writeInt(blockSize3);
        blocks.writeInt(recordCnt3);


        ByteBuf streamRanges = Unpooled.buffer(3 * (8 + 8 + 4 + 4));
        streamRanges.writeLong(streamId1);
        streamRanges.writeLong(0);
        streamRanges.writeInt(recordCnt1);
        streamRanges.writeInt(0);

        streamRanges.writeLong(streamId1);
        streamRanges.writeLong(recordCnt1);
        streamRanges.writeInt(recordCnt2);
        streamRanges.writeInt(1);

        streamRanges.writeLong(streamId1);
        streamRanges.writeLong(recordCnt1 + recordCnt2);
        streamRanges.writeInt(recordCnt2);
        streamRanges.writeInt(2);

        streamRanges.writeLong(streamId2);
        streamRanges.writeLong(110);
        streamRanges.writeInt(recordCnt3);
        streamRanges.writeInt(3);

        ObjectReader.IndexBlock indexBlock = new ObjectReader.IndexBlock(Mockito.mock(S3ObjectMetadata.class), blocks, streamRanges);

        ObjectReader.FindIndexResult rst = indexBlock.find(1, 10, 150, 100000);
        assertTrue(rst.isFulfilled());
        List<StreamDataBlock> streamDataBlocks = rst.streamDataBlocks();
        assertEquals(2, streamDataBlocks.size());
        assertEquals(0, streamDataBlocks.get(0).getBlockId());
        assertEquals(0, streamDataBlocks.get(0).getBlockStartPosition());
        assertEquals(blockSize1, streamDataBlocks.get(0).getBlockEndPosition());
        assertEquals(1, streamDataBlocks.get(1).getBlockId());
        assertEquals(blockSize1, streamDataBlocks.get(1).getBlockStartPosition());
        assertEquals((long) blockSize1 + blockSize2, streamDataBlocks.get(1).getBlockEndPosition());

        rst = indexBlock.find(1, 10, 200);
        assertTrue(rst.isFulfilled());
        assertEquals(3, rst.streamDataBlocks().size());

        rst = indexBlock.find(1L, 10, 10000, 80 * bodySize);
        assertTrue(rst.isFulfilled());
        assertEquals(3, rst.streamDataBlocks().size());

        rst = indexBlock.find(1L, 10, 10000, 160 * bodySize);
        assertFalse(rst.isFulfilled());
        assertEquals(3, rst.streamDataBlocks().size());

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
