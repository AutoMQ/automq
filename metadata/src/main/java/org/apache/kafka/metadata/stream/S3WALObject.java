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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.common.metadata.WALObjectRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public class S3WALObject implements Comparable<S3WALObject> {

    /**
     * The order id of the object.
     * Sort by this field to get the order of the objects which contains logically increasing streams.
     * <p>
     * When compact a batch of objects to a compacted object,
     * this compacted object's order id will be assigned the value <code>first object's order id in this batch</code>
     */
    private final long orderId;
    private final long objectId;

    private final int brokerId;
    private final Map<Long/*streamId*/, List<StreamOffsetRange>> streamsIndex;

    private final S3ObjectType objectType = S3ObjectType.UNKNOWN;

    private final long timestamp;

    public S3WALObject(long objectId, int brokerId, final Map<Long, List<StreamOffsetRange>> streamsIndex, long orderId) {
        this(objectId, brokerId, streamsIndex, orderId, System.currentTimeMillis());
    }

    public S3WALObject(long objectId, int brokerId, final Map<Long, List<StreamOffsetRange>> streamsIndex, long orderId, long timestamp) {
        this.orderId = orderId;
        this.objectId = objectId;
        this.brokerId = brokerId;
        this.streamsIndex = streamsIndex;
        this.timestamp = timestamp;
    }

    public boolean intersect(long streamId, long startOffset, long endOffset) {
        List<StreamOffsetRange> indexes = streamsIndex.get(streamId);
        if (indexes == null || indexes.isEmpty()) {
            return false;
        }
        StreamOffsetRange firstIndex = indexes.get(0);
        StreamOffsetRange lastIndex = indexes.get(indexes.size() - 1);
        return startOffset >= firstIndex.getStartOffset() && startOffset <= lastIndex.getEndOffset();
    }

    public Map<Long, List<StreamOffsetRange>> streamsIndex() {
        return streamsIndex;
    }

    public ApiMessageAndVersion toRecord() {
        return new ApiMessageAndVersion(new WALObjectRecord()
            .setObjectId(objectId)
            .setBrokerId(brokerId)
            .setOrderId(orderId)
            .setStreamsIndex(
                streamsIndex.values().stream().flatMap(List::stream)
                    .map(StreamOffsetRange::toRecordStreamIndex)
                    .collect(Collectors.toList())), (short) 0);
    }

    public static S3WALObject of(WALObjectRecord record) {
        Map<Long, List<StreamOffsetRange>> collect = record.streamsIndex().stream()
            .map(index -> new StreamOffsetRange(index.streamId(), index.startOffset(), index.endOffset()))
            .collect(Collectors.groupingBy(StreamOffsetRange::getStreamId));
        return new S3WALObject(record.objectId(), record.brokerId(),
            collect, record.orderId());
    }

    public Integer brokerId() {
        return brokerId;
    }

    public Long objectId() {
        return objectId;
    }

    public S3ObjectType objectType() {
        return objectType;
    }

    public long orderId() {
        return orderId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3WALObject that = (S3WALObject) o;
        return objectId == that.objectId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId);
    }

    @Override
    public String toString() {
        return "S3WALObject{" +
            "objectId=" + objectId +
            ", orderId=" + orderId +
            ", brokerId=" + brokerId +
            ", streamsIndex=" + streamsIndex +
            ", objectType=" + objectType +
            '}';
    }

    @Override
    public int compareTo(S3WALObject o) {
        return Long.compare(this.orderId, o.orderId);
    }
}
