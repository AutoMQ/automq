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

package com.automq.stream.s3.objects;

public class ObjectStreamRange {
    private long streamId;
    private long epoch;
    private long startOffset;
    private long endOffset;
    private int size;

    public ObjectStreamRange() {
    }

    public ObjectStreamRange(long streamId, long epoch, long startOffset, long endOffset, int size) {
        this.streamId = streamId;
        this.epoch = epoch;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.size = size;
    }

    public long getStreamId() {
        return streamId;
    }

    public void setStreamId(long streamId) {
        this.streamId = streamId;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
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

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "(" + streamId + "-" + epoch + "," + startOffset + "-" + endOffset + "-" + size + ")";
    }
}
