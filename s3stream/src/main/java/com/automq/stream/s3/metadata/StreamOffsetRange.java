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

    public long getStreamId() {
        return streamId;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
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
