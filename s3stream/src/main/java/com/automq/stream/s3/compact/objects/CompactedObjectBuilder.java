/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.compact.objects;

import com.automq.stream.s3.StreamDataBlock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CompactedObjectBuilder {
    private final List<StreamDataBlock> streamDataBlocks;
    private CompactionType type;
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

    public int totalStreamNum() {
        return this.streamDataBlocks.stream().map(StreamDataBlock::getStreamId).collect(Collectors.toSet()).size();
    }

    public long currStreamBlockSize() {
        return getCurrentStreamRangePositions().stream().mapToLong(StreamDataBlock::getBlockSize).sum();
    }

    public Set<Long> uniqueObjectIds() {
        return this.streamDataBlocks.stream().map(StreamDataBlock::getObjectId).collect(Collectors.toSet());
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
