/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.metadata;

import java.util.Objects;

/**
 * StreamOffsetRange represents <code>[startOffset, endOffset)</code> in the stream.
 */
public class StreamOffsetRange implements Comparable<StreamOffsetRange> {

    public static final StreamOffsetRange INVALID = new StreamOffsetRange(S3StreamConstant.INVALID_STREAM_ID,
        S3StreamConstant.INVALID_OFFSET, S3StreamConstant.INVALID_OFFSET);

    private final long streamId;

    private final long startOffset;

    private final long endOffset;

    public StreamOffsetRange(long streamId, long startOffset, long endOffset) {
        this.streamId = streamId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public long streamId() {
        return streamId;
    }

    public long startOffset() {
        return startOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    public boolean intersect(long startOffset, long endOffset) {
        return startOffset <= endOffset
            && startOffset >= this.startOffset && startOffset <= this.endOffset
            && endOffset <= this.endOffset;
    }

    @Override
    public int compareTo(StreamOffsetRange o) {
        int res = Long.compare(this.streamId, o.streamId);
        if (res != 0)
            return res;
        res = Long.compare(this.startOffset, o.startOffset);
        return res == 0 ? Long.compare(this.endOffset, o.endOffset) : res;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StreamOffsetRange that = (StreamOffsetRange) o;
        return streamId == that.streamId && startOffset == that.startOffset && endOffset == that.endOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, startOffset, endOffset);
    }

    @Override
    public String toString() {
        return "StreamOffsetRange(streamId=" + streamId + ", startOffset=" + startOffset + ", endOffset=" + endOffset + ")";
    }
}
