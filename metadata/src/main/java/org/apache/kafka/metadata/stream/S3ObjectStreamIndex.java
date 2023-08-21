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
 * ObjectStreamIndex is the index of a stream range in a WAL object or STREAM object.
 */
public class S3ObjectStreamIndex implements Comparable<S3ObjectStreamIndex> {

    private final Long streamId;

    private final Long startOffset;

    private final Long endOffset;

    public S3ObjectStreamIndex(Long streamId, Long startOffset, Long endOffset) {
        this.streamId = streamId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public Long getStreamId() {
        return streamId;
    }

    public Long getStartOffset() {
        return startOffset;
    }

    public Long getEndOffset() {
        return endOffset;
    }

    @Override
    public int compareTo(S3ObjectStreamIndex o) {
        int res = this.streamId.compareTo(o.streamId);
        return res == 0 ? this.startOffset.compareTo(o.startOffset) : res;
    }

    public StreamIndex toRecordStreamIndex() {
        return new StreamIndex()
            .setStreamId(streamId)
            .setStartOffset(startOffset)
            .setEndOffset(endOffset);
    }

    public static S3ObjectStreamIndex of(StreamIndex index) {
        return new S3ObjectStreamIndex(index.streamId(), index.startOffset(), index.endOffset());
    }
}
