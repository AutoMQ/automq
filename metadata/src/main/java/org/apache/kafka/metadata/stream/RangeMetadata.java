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

import java.util.Objects;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

/**
 * RangeMetadata is the metadata of a range of the stream.
 * <p>
 * The range represents a continuous sequence of data <code>[startOffset, endOffset)</code> in the stream.
 */
public class RangeMetadata implements Comparable<RangeMetadata> {

    /**
     * The id of the stream that the range belongs to.
     */
    private long streamId;
    /**
     * The epoch of the stream when the range is created.
     */
    private long epoch;
    /**
     * The index of the range in the stream.
     */
    private int rangeIndex;
    /**
     * Range start offer. (Inclusive)
     */
    private long startOffset;
    /**
     * Range end offer. (Exclusive)
     */
    private long endOffset;
    /**
     * The broker id of the broker that owns the range.
     */
    private int brokerId;

    public RangeMetadata(long streamId, long epoch, int rangeIndex, long startOffset, long endOffset, int brokerId) {
        this.streamId = streamId;
        this.epoch = epoch;
        this.rangeIndex = rangeIndex;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.brokerId = brokerId;
    }

    @Override
    public int compareTo(RangeMetadata o) {
        return this.rangeIndex - o.rangeIndex;
    }

    public long epoch() {
        return epoch;
    }

    public int rangeIndex() {
        return rangeIndex;
    }

    public long startOffset() {
        return startOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    public int brokerId() {
        return brokerId;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public ApiMessageAndVersion toRecord() {
        return new ApiMessageAndVersion(new RangeRecord()
            .setStreamId(streamId)
            .setEpoch(epoch)
            .setBrokerId(brokerId)
            .setRangeIndex(rangeIndex)
            .setStartOffset(startOffset)
            .setEndOffset(endOffset), (short) 0);
    }

    public static RangeMetadata of(RangeRecord record) {
        RangeMetadata rangeMetadata = new RangeMetadata(
            record.streamId(), record.epoch(), record.rangeIndex(),
            record.startOffset(), record.endOffset(), record.brokerId()
        );
        return rangeMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RangeMetadata that = (RangeMetadata) o;
        return streamId == that.streamId && epoch == that.epoch && rangeIndex == that.rangeIndex && startOffset == that.startOffset
            && endOffset == that.endOffset && brokerId == that.brokerId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, epoch, rangeIndex, startOffset, endOffset, brokerId);
    }
}
