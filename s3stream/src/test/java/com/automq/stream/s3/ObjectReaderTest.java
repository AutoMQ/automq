/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.WriteOptions;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

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
        ByteBuf indexBuf = Unpooled.buffer(3 * DataBlockIndex.BLOCK_INDEX_SIZE);
        new DataBlockIndex(streamId1, 0, recordCnt1, recordCnt1, 0, blockSize1).encode(indexBuf);
        new DataBlockIndex(streamId1, recordCnt1, recordCnt2, recordCnt2, blockSize1, blockSize2).encode(indexBuf);
        new DataBlockIndex(streamId1, recordCnt1 + recordCnt2, recordCnt3, recordCnt3, blockSize1 + blockSize2, blockSize3).encode(indexBuf);
        new DataBlockIndex(streamId2, 110, recordCnt3, recordCnt3, blockSize1 + blockSize2 + blockSize3, blockSize3).encode(indexBuf);

        ObjectReader.IndexBlock indexBlock = new ObjectReader.IndexBlock(Mockito.mock(S3ObjectMetadata.class), indexBuf);

        ObjectReader.FindIndexResult rst = indexBlock.find(1, 10, 150, 100000);
        assertTrue(rst.isFulfilled());
        List<StreamDataBlock> streamDataBlocks = rst.streamDataBlocks();
        assertEquals(2, streamDataBlocks.size());
        assertEquals(0, streamDataBlocks.get(0).getBlockStartPosition());
        assertEquals(blockSize1, streamDataBlocks.get(0).getBlockEndPosition());
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
        ObjectStorage objectStorage = new MemoryObjectStorage();
        ObjectWriter objectWriter = ObjectWriter.writer(233L, objectStorage, 1024, 1024);
        // make index block bigger than 1M
        int streamCount = 2 * 1024 * 1024 / 40;
        for (int i = 0; i < streamCount; i++) {
            StreamRecordBatch r = StreamRecordBatch.of(i, 0, i, 1, TestUtils.random(1));
            objectWriter.write(i, List.of(r));
        }
        objectWriter.close().get();
        S3ObjectMetadata metadata = new S3ObjectMetadata(233L, objectWriter.size(), S3ObjectType.STREAM_SET);
        try (ObjectReader objectReader = ObjectReader.reader(metadata, objectStorage)) {
            ObjectReader.BasicObjectInfo info = objectReader.basicObjectInfo().get();
            assertEquals(streamCount, info.indexBlock().count());
        }
    }

    @Test
    public void testReadBlockGroup() throws ExecutionException, InterruptedException {
        ObjectStorage objectStorage = new MemoryObjectStorage();
        ByteBuf buf = ByteBufAlloc.byteBuffer(0);
        buf.writeBytes(new ObjectWriter.DataBlock(233L, List.of(
            StreamRecordBatch.of(233L, 0, 10, 1, TestUtils.random(100)),
            StreamRecordBatch.of(233L, 0, 11, 2, TestUtils.random(100))
        )).buffer());
        buf.writeBytes(new ObjectWriter.DataBlock(233L, List.of(
            StreamRecordBatch.of(233L, 0, 13, 1, TestUtils.random(100))
        )).buffer());
        int indexPosition = buf.readableBytes();
        new DataBlockIndex(233L, 10, 4, 3, 0, buf.readableBytes()).encode(buf);
        int indexSize = buf.readableBytes() - indexPosition;
        buf.writeBytes(new ObjectWriter.Footer(indexPosition, indexSize).buffer());
        int objectSize = buf.readableBytes();
        objectStorage.write(WriteOptions.DEFAULT, ObjectUtils.genKey(0, 1L), buf).get();
        try (ObjectReader reader = ObjectReader.reader(new S3ObjectMetadata(1L, objectSize, S3ObjectType.STREAM), objectStorage)) {
            ObjectReader.FindIndexResult rst = reader.find(233L, 10L, 14L, 1024).get();
            assertEquals(1, rst.streamDataBlocks().size());
            try (ObjectReader.DataBlockGroup dataBlockGroup = reader.read(rst.streamDataBlocks().get(0).dataBlockIndex()).get()) {
                assertEquals(3, dataBlockGroup.recordCount());
                Iterator<StreamRecordBatch> it = dataBlockGroup.iterator();
                assertEquals(10, it.next().getBaseOffset());
                assertEquals(11, it.next().getBaseOffset());
                assertEquals(13, it.next().getBaseOffset());
            }
        }
    }

    @Test
    public void testStreamOffsetRange() throws ExecutionException, InterruptedException {
        // prepare data
        S3ObjectMetadata metadata = new S3ObjectMetadata(1, 0, S3ObjectType.STREAM_SET);
        ObjectStorage objectStorage = new MemoryObjectStorage();
        ObjectWriter objectWriter = ObjectWriter.writer(1, objectStorage, 1024, 1024);
        {
            StreamRecordBatch r = newRecord(200, 10, 5, 512);
            objectWriter.write(200, List.of(r));
        }
        {
            StreamRecordBatch r1 = newRecord(234, 0, 5, 512);
            objectWriter.write(234, List.of(r1));
            StreamRecordBatch r2 = newRecord(234, 5, 20, 512);
            objectWriter.write(234, List.of(r2));
        }
        {
            StreamRecordBatch r = newRecord(250, 30, 5, 512);
            objectWriter.write(250, List.of(r));
        }
        objectWriter.close().get();
        metadata.setObjectSize(objectWriter.size());

        ObjectReader objectReader = ObjectReader.reader(metadata, objectStorage);
        ObjectReader.BasicObjectInfo info = objectReader.basicObjectInfo().get();

        // check find
        assertEquals(new StreamOffsetRange(200, 10, 15), info.indexBlock().findStreamOffsetRange(200).get());
        assertEquals(new StreamOffsetRange(234, 0, 25), info.indexBlock().findStreamOffsetRange(234).get());
        assertEquals(new StreamOffsetRange(250, 30, 35), info.indexBlock().findStreamOffsetRange(250).get());
        assertTrue(info.indexBlock().findStreamOffsetRange(100).isEmpty());
        assertTrue(info.indexBlock().findStreamOffsetRange(230).isEmpty());
        assertTrue(info.indexBlock().findStreamOffsetRange(300).isEmpty());

        // check ranges
        assertEquals(
            List.of(
                new StreamOffsetRange(200, 10, 15),
                new StreamOffsetRange(234, 0, 25),
                new StreamOffsetRange(250, 30, 35)
            ),
            info.indexBlock().streamOffsetRanges()
        );
    }

    StreamRecordBatch newRecord(long streamId, long offset, int count, int payloadSize) {
        return StreamRecordBatch.of(streamId, 0, offset, count, TestUtils.random(payloadSize));
    }
}
