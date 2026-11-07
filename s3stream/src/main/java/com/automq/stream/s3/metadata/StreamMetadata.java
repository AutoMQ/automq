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

package com.automq.stream.s3.metadata;

public class StreamMetadata {
    private long streamId;
    private long epoch;
    private long startOffset;
    private long endOffset;
    private StreamState state;
    private int nodeId = -1;

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

    public int nodeId() {
        return nodeId;
    }

    public void nodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public String toString() {
        return "StreamMetadata{" +
            "streamId=" + streamId +
            ", epoch=" + epoch +
            ", startOffset=" + startOffset +
            ", endOffset=" + endOffset +
            ", state=" + state +
            ", nodeId=" + nodeId +
            '}';
    }
}
