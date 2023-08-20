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

import java.util.Optional;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public class RangeMetadata implements Comparable<RangeMetadata> {
    private Long streamId;
    private Integer epoch;
    private Integer rangeIndex;
    private Long startOffset;
    private Optional<Long> endOffset;
    private Integer brokerId;
    @Override
    public int compareTo(RangeMetadata o) {
        return this.rangeIndex.compareTo(o.rangeIndex);
    }

    public Integer getEpoch() {
        return epoch;
    }

    public Integer getRangeIndex() {
        return rangeIndex;
    }

    public Long getStartOffset() {
        return startOffset;
    }

    public Optional<Long> getEndOffset() {
        return endOffset;
    }

    public Integer getBrokerId() {
        return brokerId;
    }

    public ApiMessageAndVersion toRecord() {
        return new ApiMessageAndVersion(new RangeRecord()
            .setStreamId(streamId)
            .setEpoch(epoch)
            .setBrokerId(brokerId)
            .setRangeIndex(rangeIndex)
            .setStartOffset(startOffset)
            .setEndOffset(endOffset.get()), (short) 0);
    }

    public static RangeMetadata of(RangeRecord record) {
        RangeMetadata rangeMetadata = new RangeMetadata();
        rangeMetadata.streamId = record.streamId();
        rangeMetadata.epoch = record.epoch();
        rangeMetadata.rangeIndex = record.rangeIndex();
        rangeMetadata.startOffset = record.startOffset();
        rangeMetadata.endOffset = Optional.ofNullable(record.endOffset());
        rangeMetadata.brokerId = record.brokerId();
        return rangeMetadata;
    }
}
