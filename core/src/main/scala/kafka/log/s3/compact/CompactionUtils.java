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

import kafka.log.s3.compact.objects.CompactedObject;
import kafka.log.s3.compact.objects.StreamDataBlock;
import kafka.log.s3.compact.operator.DataBlockReader;
import kafka.log.s3.objects.ObjectStreamRange;

import java.util.ArrayList;
import java.util.List;

public class CompactionUtils {
    public static List<ObjectStreamRange> buildObjectStreamRange(CompactedObject compactedObject) {
        List<ObjectStreamRange> objectStreamRanges = new ArrayList<>();
        ObjectStreamRange currObjectStreamRange = null;
        for (StreamDataBlock streamDataBlock : compactedObject.streamDataBlocks()) {
            if (currObjectStreamRange == null) {
                currObjectStreamRange = new ObjectStreamRange(streamDataBlock.getStreamId(), -1L,
                        streamDataBlock.getStartOffset(), streamDataBlock.getEndOffset());
            } else {
                if (currObjectStreamRange.getStreamId() == streamDataBlock.getStreamId()) {
                    currObjectStreamRange.setEndOffset(streamDataBlock.getEndOffset());
                } else {
                    objectStreamRanges.add(currObjectStreamRange);
                    currObjectStreamRange = new ObjectStreamRange(streamDataBlock.getStreamId(), -1L,
                            streamDataBlock.getStartOffset(), streamDataBlock.getEndOffset());
                }
            }
        }
        objectStreamRanges.add(currObjectStreamRange);
        return objectStreamRanges;
    }

    public static List<DataBlockReader.DataBlockIndex> buildBlockIndicesFromStreamDataBlock(List<StreamDataBlock> streamDataBlocks) {
        List<DataBlockReader.DataBlockIndex> blockIndices = new ArrayList<>();
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            blockIndices.add(new DataBlockReader.DataBlockIndex(streamDataBlock.getBlockId(), streamDataBlock.getBlockPosition(),
                    streamDataBlock.getBlockSize(), streamDataBlock.getRecordCount()));
        }
        return blockIndices;
    }
}
