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

    public StreamState getState() {
        return state;
    }

    public void setState(StreamState state) {
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
