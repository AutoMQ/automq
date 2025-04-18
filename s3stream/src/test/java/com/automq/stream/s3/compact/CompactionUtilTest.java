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

package com.automq.stream.s3.compact;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.compact.objects.CompactedObject;
import com.automq.stream.s3.compact.objects.CompactionType;
import com.automq.stream.s3.compact.utils.CompactionUtils;
import com.automq.stream.s3.compact.utils.GroupByLimitPredicate;
import com.automq.stream.s3.compact.utils.GroupByOffsetPredicate;
import com.automq.stream.s3.objects.ObjectStreamRange;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static com.automq.stream.s3.ByteBufAllocPolicy.POOLED_DIRECT;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(30)
@Tag("S3Unit")
public class CompactionUtilTest extends CompactionTestBase {

    @BeforeEach
    public void setUp() throws Exception {
        ByteBufAlloc.setPolicy(POOLED_DIRECT);
    }

    @Test
    public void testMergeStreamDataBlocks() {
        List<StreamDataBlock> streamDataBlocks = List.of(
            new StreamDataBlock(STREAM_0, 0, 15, 1, 0, 20, 1),
            new StreamDataBlock(STREAM_0, 15, 30, 1, 20, 5, 1),
            new StreamDataBlock(STREAM_0, 30, 100, 1, 25, 80, 1),
            new StreamDataBlock(STREAM_2, 40, 100, 1, 105, 80, 1),
            new StreamDataBlock(STREAM_2, 120, 150, 1, 185, 30, 1));
        List<List<StreamDataBlock>> result = CompactionUtils.groupStreamDataBlocks(streamDataBlocks, new GroupByOffsetPredicate());
        assertEquals(3, result.size());
        Assertions.assertEquals(List.of(streamDataBlocks.get(0), streamDataBlocks.get(1), streamDataBlocks.get(2)), result.get(0));
        Assertions.assertEquals(List.of(streamDataBlocks.get(3)), result.get(1));
        Assertions.assertEquals(List.of(streamDataBlocks.get(4)), result.get(2));
    }

    @Test
    public void testMergeStreamDataBlocks2() {
        List<StreamDataBlock> streamDataBlocks = List.of(
            new StreamDataBlock(STREAM_0, 0, 15, 1, 0, 20, 1),
            new StreamDataBlock(STREAM_0, 15, 30, 1, 20, 5, 1),
            new StreamDataBlock(STREAM_0, 30, 100, 1, 25, 80, 1),
            new StreamDataBlock(STREAM_2, 40, 100, 1, 105, 80, 1),
            new StreamDataBlock(STREAM_2, 120, 150, 1, 185, 30, 1));

        List<List<StreamDataBlock>> result = CompactionUtils.groupStreamDataBlocks(streamDataBlocks, new GroupByLimitPredicate(30));
        assertEquals(4, result.size());
        Assertions.assertEquals(List.of(streamDataBlocks.get(0), streamDataBlocks.get(1)), result.get(0));
        Assertions.assertEquals(List.of(streamDataBlocks.get(2)), result.get(1));
        Assertions.assertEquals(List.of(streamDataBlocks.get(3)), result.get(2));
        Assertions.assertEquals(List.of(streamDataBlocks.get(4)), result.get(3));
    }

    @Test
    public void testBuildObjectStreamRanges() {
        List<StreamDataBlock> streamDataBlocks = List.of(
            new StreamDataBlock(STREAM_0, 0, 20, 1, 0, 20, 1),
            new StreamDataBlock(STREAM_0, 20, 25, 0, 20, 5, 1),
            new StreamDataBlock(STREAM_2, 40, 120, 2, 25, 80, 1),
            new StreamDataBlock(STREAM_2, 120, 150, 3, 105, 30, 1));
        CompactedObject compactedObject = new CompactedObject(CompactionType.COMPACT, streamDataBlocks);
        List<ObjectStreamRange> result = CompactionUtils.buildObjectStreamRangeFromGroup(
            CompactionUtils.groupStreamDataBlocks(compactedObject.streamDataBlocks(), new GroupByOffsetPredicate()));
        assertEquals(2, result.size());
        assertEquals(STREAM_0, result.get(0).getStreamId());
        assertEquals(0, result.get(0).getStartOffset());
        assertEquals(25, result.get(0).getEndOffset());
        assertEquals(STREAM_2, result.get(1).getStreamId());
        assertEquals(40, result.get(1).getStartOffset());
        assertEquals(150, result.get(1).getEndOffset());
    }

    @Test
    public void testBuildDataIndices() {
        List<StreamDataBlock> streamDataBlocks = List.of(
            new StreamDataBlock(STREAM_0, 0, 15, 1, 0, 20, 1),
            new StreamDataBlock(STREAM_0, 15, 30, 1, 20, 5, 2),
            new StreamDataBlock(STREAM_0, 30, 100, 1, 25, 80, 3),
            new StreamDataBlock(STREAM_2, 40, 100, 1, 105, 80, 4),
            new StreamDataBlock(STREAM_2, 120, 150, 1, 185, 30, 5));
        CompactedObject compactedObject = new CompactedObject(CompactionType.COMPACT, streamDataBlocks);
        List<DataBlockIndex> result = CompactionUtils.buildDataBlockIndicesFromGroup(
            CompactionUtils.groupStreamDataBlocks(compactedObject.streamDataBlocks(), new GroupByLimitPredicate(30)));

        assertEquals(4, result.size());
        assertEquals(new DataBlockIndex(STREAM_0, 0, 30, 3, 0, 25), result.get(0));
        assertEquals(new DataBlockIndex(STREAM_0, 30, 70, 3, 25, 80), result.get(1));
        assertEquals(new DataBlockIndex(STREAM_2, 40, 60, 4, 105, 80), result.get(2));
        assertEquals(new DataBlockIndex(STREAM_2, 120, 30, 5, 185, 30), result.get(3));
    }

    @Test
    public void testBuildDataIndices2() {
        List<StreamDataBlock> streamDataBlocks = List.of(
            new StreamDataBlock(STREAM_0, 0, 15, 1, 0, 20, 1),
            new StreamDataBlock(STREAM_0, 15, 30, 1, 20, 5, 2),
            new StreamDataBlock(STREAM_0, 30, (long) (Integer.MAX_VALUE) + 30, 1, 25, 80, 3),
            new StreamDataBlock(STREAM_2, 40, 100, 1, 105, 80, 4),
            new StreamDataBlock(STREAM_2, 120, 150, 1, 185, 30, 5));
        CompactedObject compactedObject = new CompactedObject(CompactionType.COMPACT, streamDataBlocks);
        List<DataBlockIndex> result = CompactionUtils.buildDataBlockIndicesFromGroup(
            CompactionUtils.groupStreamDataBlocks(compactedObject.streamDataBlocks(), new GroupByLimitPredicate(999)));

        assertEquals(4, result.size());
        assertEquals(new DataBlockIndex(STREAM_0, 0, 30, 3, 0, 25), result.get(0));
        assertEquals(new DataBlockIndex(STREAM_0, 30, Integer.MAX_VALUE, 3, 25, 80), result.get(1));
        assertEquals(new DataBlockIndex(STREAM_2, 40, 60, 4, 105, 80), result.get(2));
        assertEquals(new DataBlockIndex(STREAM_2, 120, 30, 5, 185, 30), result.get(3));
    }

    @Test
    public void testBuildDataIndices3() {
        List<StreamDataBlock> streamDataBlocks = List.of(
            new StreamDataBlock(STREAM_0, 0, 15, 1, 0, 20, 1),
            new StreamDataBlock(STREAM_0, 15, 30, 1, 20, 5, 2),
            new StreamDataBlock(STREAM_0, 30, 100, 1, 25, 80, Integer.MAX_VALUE),
            new StreamDataBlock(STREAM_2, 40, 100, 1, 105, 80, 4),
            new StreamDataBlock(STREAM_2, 120, 150, 1, 185, 30, 5));
        CompactedObject compactedObject = new CompactedObject(CompactionType.COMPACT, streamDataBlocks);
        List<DataBlockIndex> result = CompactionUtils.buildDataBlockIndicesFromGroup(
            CompactionUtils.groupStreamDataBlocks(compactedObject.streamDataBlocks(), new GroupByLimitPredicate(999)));

        assertEquals(4, result.size());
        assertEquals(new DataBlockIndex(STREAM_0, 0, 30, 3, 0, 25), result.get(0));
        assertEquals(new DataBlockIndex(STREAM_0, 30, 70, Integer.MAX_VALUE, 25, 80), result.get(1));
        assertEquals(new DataBlockIndex(STREAM_2, 40, 60, 4, 105, 80), result.get(2));
        assertEquals(new DataBlockIndex(STREAM_2, 120, 30, 5, 185, 30), result.get(3));
    }
}
