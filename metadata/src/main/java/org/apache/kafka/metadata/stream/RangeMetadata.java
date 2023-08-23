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

import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public class RangeMetadata implements Comparable<RangeMetadata> {
    private long streamId;
    private long epoch;
    private int rangeIndex;
    /**
     * Inclusive
     */
    private long startOffset;
    /**
     * Exclusive
     */
    private long endOffset;
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
}
