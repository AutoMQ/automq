/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.objects;

import java.util.List;

public class CompactStreamObjectRequest {
    private long objectId;
    private long objectSize;
    private long streamId;
    private long startOffset;
    private long endOffset;
    private final long streamEpoch;
    /**
     * The generated object's attributes.
     */
    private final int attributes;
    /**
     * The source objects' id of the stream object.
     */
    private final List<Long> sourceObjectIds;

    public CompactStreamObjectRequest(
        long objectId,
        long objectSize,
        long streamId,
        long streamEpoch,
        long startOffset,
        long endOffset,
        List<Long> sourceObjectIds,
        int attributes) {
        this.objectId = objectId;
        this.objectSize = objectSize;
        this.streamId = streamId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.streamEpoch = streamEpoch;
        this.sourceObjectIds = sourceObjectIds;
        this.attributes = attributes;
    }

    public long getObjectId() {
        return objectId;
    }

    public void setObjectId(long objectId) {
        this.objectId = objectId;
    }

    public long getObjectSize() {
        return objectSize;
    }

    public void setObjectSize(long objectSize) {
        this.objectSize = objectSize;
    }

    public long getStreamId() {
        return streamId;
    }

    public void setStreamId(long streamId) {
        this.streamId = streamId;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public long getStreamEpoch() {
        return streamEpoch;
    }

    public List<Long> getSourceObjectIds() {
        return sourceObjectIds;
    }

    public int getAttributes() {
        return attributes;
    }

    @Override
    public String toString() {
        return "CommitStreamObjectRequest{" +
            "objectId=" + objectId +
            ", objectSize=" + objectSize +
            ", streamId=" + streamId +
            ", startOffset=" + startOffset +
            ", endOffset=" + endOffset +
            ", streamEpoch=" + streamEpoch +
            ", sourceObjectIds=" + sourceObjectIds +
            '}';
    }
}
