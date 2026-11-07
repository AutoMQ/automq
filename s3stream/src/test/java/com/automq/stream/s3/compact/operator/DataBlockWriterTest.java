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

package com.automq.stream.s3.compact.operator;

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.Writer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies compaction write grouping with large logical sizes and tiny physical buffers.
 */
@Tag("S3Unit")
class DataBlockWriterTest {
    /**
     * A third block that would overflow the combined buffer flushes the first two before being queued.
     */
    @Test
    void testFlushesBeforeExceedingBufferCapacity() {
        ObjectStorage storage = mock(ObjectStorage.class);
        Writer writer = mock(Writer.class);
        List<byte[]> writes = new ArrayList<>();
        List<ByteBuf> buffers = new ArrayList<>();
        when(storage.writer(any(), anyString())).thenReturn(writer);
        when(writer.close()).thenReturn(CompletableFuture.completedFuture(null));
        when(writer.write(any())).thenAnswer(invocation -> {
            ByteBuf buffer = invocation.getArgument(0);
            byte[] bytes = new byte[buffer.readableBytes()];
            buffer.readBytes(bytes);
            writes.add(bytes);
            buffer.release();
            return CompletableFuture.completedFuture(null);
        });
        DataBlockWriter blockWriter = new DataBlockWriter(1, storage, Integer.MAX_VALUE);
        int blockSize = 900 * 1024 * 1024;
        for (int i = 0; i < 3; i++) {
            StreamDataBlock block = new StreamDataBlock(42, i, i + 1, 1, (long) i * blockSize, blockSize, 1);
            ByteBuf data = Unpooled.buffer(1).writeByte(i + 1);
            buffers.add(data);
            block.getDataCf().complete(data);
            blockWriter.write(block);
        }
        assertEquals(1, writes.size());
        assertArrayEquals(new byte[] {1, 2}, writes.get(0));
        blockWriter.close().join();
        assertEquals(2, writes.size());
        assertEquals(3, writes.get(1)[0]);
        assertEquals(1 + 3 * DataBlockIndex.BLOCK_INDEX_SIZE + 48, writes.get(1).length);
        assertEquals(3L * blockSize + 3 * DataBlockIndex.BLOCK_INDEX_SIZE + 48, blockWriter.size());
        buffers.forEach(buffer -> assertEquals(0, buffer.refCnt()));
    }
}
