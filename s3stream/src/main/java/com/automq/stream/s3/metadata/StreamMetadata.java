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

package com.automq.stream.s3.metadata;

public class StreamMetadata {
    private long streamId;
    private long epoch;
    private long startOffset;
    private long endOffset;
    private StreamState state;

    @SuppressWarnings("unused")
    public StreamMetadata() {
    }

    public StreamMetadata(long streamId, long epoch, long startOffset, long endOffset, StreamState state) {
        this.streamId = streamId;
        this.epoch = epoch;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.state = state;
    }

    public long streamId() {
        return streamId;
    }

    public void streamId(long streamId) {
        this.streamId = streamId;
    }

    public long epoch() {
        return epoch;
    }

    public void epoch(long epoch) {
        this.epoch = epoch;
    }

    public long startOffset() {
        return startOffset;
    }

    public void startOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    public void endOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public StreamState state() {
        return state;
    }

    public void state(StreamState state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "StreamMetadata{" +
            "streamId=" + streamId +
            ", epoch=" + epoch +
            ", startOffset=" + startOffset +
            ", endOffset=" + endOffset +
            ", state=" + state +
            '}';
    }
}
