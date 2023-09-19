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

package kafka.log.stream.s3.compact.objects;

import java.util.ArrayList;
import java.util.List;

public class CompactedObjectBuilder {
    private CompactionType type;
    private final List<StreamDataBlock> streamDataBlocks;
    private int currStreamIndexHead;
    private int currStreamIndexTail;

    public CompactedObjectBuilder() {
        this.type = CompactionType.COMPACT;
        this.streamDataBlocks = new ArrayList<>();
        this.currStreamIndexHead = -1;
        this.currStreamIndexTail = -1;
    }

    public CompactedObjectBuilder splitCurrentStream() {
        return split(currStreamIndexHead, currStreamIndexTail);
    }

    public CompactedObjectBuilder split(int start, int end) {
        if (start < 0 || end > currStreamIndexTail) {
            // split out of range
            return new CompactedObjectBuilder();
        }
        CompactedObjectBuilder builder = new CompactedObjectBuilder();
        List<StreamDataBlock> streamRangePositionsSubList = streamDataBlocks.subList(start, end);
        for (StreamDataBlock streamRangePosition : streamRangePositionsSubList) {
            builder.addStreamDataBlock(streamRangePosition);
        }
        builder.setType(type);
        streamRangePositionsSubList.clear();
        resetCurrStreamPosition();
        return builder;
    }

    private void resetCurrStreamPosition() {
        currStreamIndexHead = -1;
        currStreamIndexTail = -1;
        long currStreamId = -1;
        for (int i = 0; i < streamDataBlocks.size(); i++) {
            StreamDataBlock streamDataBlock = streamDataBlocks.get(i);
            if (currStreamId != streamDataBlock.getStreamId()) {
                currStreamId = streamDataBlock.getStreamId();
                currStreamIndexHead = i;
            }
            currStreamIndexTail = i + 1;
        }
    }

    private List<StreamDataBlock> getCurrentStreamRangePositions() {
        if (currStreamIndexHead == -1 || currStreamIndexTail == -1) {
            return new ArrayList<>();
        }
        return streamDataBlocks.subList(currStreamIndexHead, currStreamIndexTail);
    }

    public CompactedObjectBuilder setType(CompactionType type) {
        this.type = type;
        return this;
    }

    public CompactionType type() {
        return this.type;
    }

    public long lastStreamId() {
        if (streamDataBlocks.isEmpty()) {
            return -1;
        }
        return streamDataBlocks.get(streamDataBlocks.size() - 1).getStreamId();
    }

    public long lastOffset() {
        if (streamDataBlocks.isEmpty()) {
            return -1;
        }
        return streamDataBlocks.get(streamDataBlocks.size() - 1).getEndOffset();
    }

    public CompactedObjectBuilder addStreamDataBlock(StreamDataBlock streamDataBlock) {
        if (streamDataBlock.getStreamId() != lastStreamId()) {
            this.currStreamIndexHead = this.streamDataBlocks.size();
        }
        this.streamDataBlocks.add(streamDataBlock);
        this.currStreamIndexTail = this.streamDataBlocks.size();
        return this;
    }

    public List<StreamDataBlock> streamDataBlocks() {
        return this.streamDataBlocks;
    }

    public long currStreamBlockSize() {
        return getCurrentStreamRangePositions().stream().mapToLong(StreamDataBlock::getBlockSize).sum();
    }

    public long totalBlockSize() {
        return streamDataBlocks.stream().mapToLong(StreamDataBlock::getBlockSize).sum();
    }

    public void merge(CompactedObjectBuilder other) {
        if (other.type == CompactionType.SPLIT) {
            // cannot merge compacted object of split type as split strategy is determined when constructing compacted objects
            return;
        }
        this.streamDataBlocks.addAll(other.streamDataBlocks);
        resetCurrStreamPosition();
    }

    public CompactedObject build() {
        return new CompactedObject(type, streamDataBlocks);
    }
}
