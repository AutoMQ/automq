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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

        List<ObjectReader.DataBlockIndex> rst = indexBlock.find(1, 10, 300, 100000);
        assertEquals(2, rst.size());
        assertEquals(0, rst.get(0).blockId());
        assertEquals(0, rst.get(0).startPosition());
        assertEquals(1024, rst.get(0).endPosition());
        assertEquals(1, rst.get(1).blockId());
        assertEquals(1024, rst.get(1).startPosition());
        assertEquals(1536, rst.get(1).endPosition());

        rst = indexBlock.find(1, 10, 400);
        assertEquals(3, rst.size());

        rst = indexBlock.find(1, 10, 400, 10);
        assertEquals(2, rst.size());
    }

}
