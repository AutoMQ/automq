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

package org.apache.kafka.metadata.stream;

import org.apache.kafka.common.metadata.WALObjectRecord.StreamIndex;

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

    @Override
    public int compareTo(StreamOffsetRange o) {
        int res = Long.compare(this.streamId, o.streamId);
        return res == 0 ? Long.compare(this.startOffset, o.startOffset) : res;
    }

    public StreamIndex toRecordStreamIndex() {
        return new StreamIndex()
            .setStreamId(streamId)
            .setStartOffset(startOffset)
            .setEndOffset(endOffset);
    }

    public static StreamOffsetRange of(StreamIndex index) {
        return new StreamOffsetRange(index.streamId(), index.startOffset(), index.endOffset());
    }
}
