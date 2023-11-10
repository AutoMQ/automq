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

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public class S3SSTObject implements Comparable<S3SSTObject> {

    private final long objectId;
    private final int nodeId;
    private final Map<Long/*streamId*/, StreamOffsetRange> streamOffsetRanges;

    /**
     * The order id of the object. Sort by this field to get the order of the objects which contains logically increasing streams.
     * <p>
     * When compact a batch of objects to a compacted object, this compacted object's order id will be assigned the value <code>first object's order
     * id in this batch</code>
     */
    private final long orderId;
    private final long dataTimeInMs;

    // Only used for testing
    public S3SSTObject(long objectId, int nodeId, final Map<Long, StreamOffsetRange> streamOffsetRanges, long orderId) {
        this(objectId, nodeId, streamOffsetRanges, orderId, S3StreamConstant.INVALID_TS);
    }

    public S3SSTObject(long objectId, int nodeId, final Map<Long, StreamOffsetRange> streamOffsetRanges, long orderId, long dataTimeInMs) {
        this.orderId = orderId;
        this.objectId = objectId;
        this.nodeId = nodeId;
        this.streamOffsetRanges = streamOffsetRanges;
        this.dataTimeInMs = dataTimeInMs;
    }

    public boolean intersect(long streamId, long startOffset, long endOffset) {
        StreamOffsetRange offsetRange = streamOffsetRanges.get(streamId);
        if (offsetRange == null) {
            return false;
        }
        return startOffset >= offsetRange.getStartOffset() && startOffset <= offsetRange.getEndOffset();
    }

    public Map<Long, StreamOffsetRange> offsetRanges() {
        return streamOffsetRanges;
    }

    public ApiMessageAndVersion toRecord() {
        return new ApiMessageAndVersion(new S3StreamSetObjectRecord()
            .setObjectId(objectId)
            .setNodeId(nodeId)
            .setOrderId(orderId)
            .setDataTimeInMs(dataTimeInMs)
            .setStreamsIndex(
                streamOffsetRanges
                    .values()
                    .stream()
                    .map(Convertor::to)
                    .collect(Collectors.toList())), (short) 0);
    }

    public static S3SSTObject of(S3StreamSetObjectRecord record) {
        Map<Long, StreamOffsetRange> offsetRanges = record.streamsIndex()
            .stream()
            .collect(Collectors.toMap(S3StreamSetObjectRecord.StreamIndex::streamId,
                index -> new StreamOffsetRange(index.streamId(), index.startOffset(), index.endOffset())));
        return new S3SSTObject(record.objectId(), record.nodeId(),
            offsetRanges, record.orderId(), record.dataTimeInMs());
    }

    public Integer nodeId() {
        return nodeId;
    }

    public Long objectId() {
        return objectId;
    }

    public S3ObjectType objectType() {
        return S3ObjectType.STREAM_SET;
    }

    public long orderId() {
        return orderId;
    }

    public long dataTimeInMs() {
        return dataTimeInMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3SSTObject that = (S3SSTObject) o;
        return objectId == that.objectId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId);
    }

    @Override
    public String toString() {
        return "S3SSTObject{" +
            "objectId=" + objectId +
            ", orderId=" + orderId +
            ", nodeId=" + nodeId +
            ", streamOffsetRanges=" + streamOffsetRanges +
            ", dataTimeInMs=" + dataTimeInMs +
            '}';
    }

    @Override
    public int compareTo(S3SSTObject o) {
        return Long.compare(this.orderId, o.orderId);
    }
}
