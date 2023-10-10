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

import com.automq.stream.s3.compact.objects.CompactedObject;
import com.automq.stream.s3.compact.objects.CompactionType;
import com.automq.stream.s3.compact.objects.StreamDataBlock;
import com.automq.stream.s3.objects.ObjectStreamRange;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(30)
@Tag("S3Unit")
public class CompactionUtilTest extends CompactionTestBase {

    @Test
    public void testBuildObjectStreamRanges() {
        List<StreamDataBlock> streamDataBlocks = List.of(
                new StreamDataBlock(STREAM_0, 0, 20, 2, 1, 0, 20, 1),
                new StreamDataBlock(STREAM_0, 20, 25, 3, 0, 20, 5, 1),
                new StreamDataBlock(STREAM_2, 40, 120, 0, 2, 25, 80, 1),
                new StreamDataBlock(STREAM_2, 120, 150, 1, 3, 105, 30, 1));
        CompactedObject compactedObject = new CompactedObject(CompactionType.COMPACT, streamDataBlocks);
        List<ObjectStreamRange> result = CompactionUtils.buildObjectStreamRange(compactedObject.streamDataBlocks());
        assertEquals(2, result.size());
        assertEquals(STREAM_0, result.get(0).getStreamId());
        assertEquals(0, result.get(0).getStartOffset());
        assertEquals(25, result.get(0).getEndOffset());
        assertEquals(STREAM_2, result.get(1).getStreamId());
        assertEquals(40, result.get(1).getStartOffset());
        assertEquals(150, result.get(1).getEndOffset());
    }

    @Test
    public void testMergeStreamDataBlocks() {
        List<StreamDataBlock> streamDataBlocks = List.of(
                new StreamDataBlock(STREAM_0, 0, 15, 0, 1, 0, 20, 1),
                new StreamDataBlock(STREAM_0, 15, 30, 1, 1, 20, 5, 1),
                new StreamDataBlock(STREAM_2, 40, 100, 2, 1, 25, 80, 1),
                new StreamDataBlock(STREAM_2, 120, 150, 3, 1, 105, 30, 1));
        List<StreamDataBlock> result = CompactionUtils.mergeStreamDataBlocks(streamDataBlocks);
        assertEquals(3, result.size());
        assertEquals(STREAM_0, result.get(0).getStreamId());
        assertEquals(0, result.get(0).getStartOffset());
        assertEquals(30, result.get(0).getEndOffset());
        assertEquals(0, result.get(0).getBlockStartPosition());
        assertEquals(25, result.get(0).getBlockEndPosition());
        assertEquals(STREAM_2, result.get(1).getStreamId());
        assertEquals(40, result.get(1).getStartOffset());
        assertEquals(100, result.get(1).getEndOffset());
        assertEquals(25, result.get(1).getBlockStartPosition());
        assertEquals(105, result.get(1).getBlockEndPosition());
        assertEquals(STREAM_2, result.get(2).getStreamId());
        assertEquals(120, result.get(2).getStartOffset());
        assertEquals(150, result.get(2).getEndOffset());
        assertEquals(105, result.get(2).getBlockStartPosition());
        assertEquals(135, result.get(2).getBlockEndPosition());
    }
}
