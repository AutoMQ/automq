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


import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class S3ObjectMetadata {

    private final long objectId;

    /**
     * order id of the object.
     * <ul>
     *     <li> WAL object: order id of the wal object.
     *     <li> STREAM object: meaningless.
     * </ul>
     */
    private final long orderId;
    private long objectSize;
    private final S3ObjectType type;
    /**
     * stream offset ranges of the object.
     * <ul>
     *     <li> WAL object: one or more stream offset ranges.
     *     <li> STREAM object: only one stream offset range.
     * </ul>
     */
    private final List<StreamOffsetRange> offsetRanges;
    /**
     * real committed timestamp of the data in the object.
     */
    private long committedTimestamp;

    /**
     * logical timestamp in ms of the data in the object.
     */
    private final long dataTimeInMs;

    // Only used for testing
    public S3ObjectMetadata(long objectId, long objectSize, S3ObjectType type) {
        this(objectId, type, Collections.emptyList(), S3StreamConstant.INVALID_TS, S3StreamConstant.INVALID_TS, objectSize,
            S3StreamConstant.INVALID_ORDER_ID);
    }

    public S3ObjectMetadata(long objectId, S3ObjectType type, List<StreamOffsetRange> offsetRanges, long dataTimeInMs) {
        this(objectId, type, offsetRanges, dataTimeInMs, S3StreamConstant.INVALID_TS, S3StreamConstant.INVALID_OBJECT_SIZE,
            S3StreamConstant.INVALID_ORDER_ID);
    }

    public S3ObjectMetadata(long objectId, S3ObjectType type, List<StreamOffsetRange> offsetRanges, long dataTimeInMs,
        long orderId) {
        this(objectId, type, offsetRanges, dataTimeInMs, S3StreamConstant.INVALID_TS, S3StreamConstant.INVALID_OBJECT_SIZE,
            orderId);
    }

    public S3ObjectMetadata(
        // these four params come from S3WALObject or S3StreamObject
        long objectId, S3ObjectType type, List<StreamOffsetRange> offsetRanges, long dataTimeInMs,
        // these two params come from S3Object
        long committedTimestamp, long objectSize,
        // this param only comes from S3WALObject
        long orderId) {
        this.objectId = objectId;
        this.orderId = orderId;
        this.objectSize = objectSize;
        this.type = type;
        this.offsetRanges = offsetRanges;
        this.dataTimeInMs = dataTimeInMs;
        this.committedTimestamp = committedTimestamp;
    }

    public void setObjectSize(long objectSize) {
        this.objectSize = objectSize;
    }

    public void setCommittedTimestamp(long committedTimestamp) {
        this.committedTimestamp = committedTimestamp;
    }

    public long objectId() {
        return objectId;
    }

    public long objectSize() {
        return objectSize;
    }

    public S3ObjectType getType() {
        return type;
    }

    public long getOrderId() {
        return orderId;
    }

    public long committedTimestamp() {
        return committedTimestamp;
    }

    public long dataTimeInMs() {
        return dataTimeInMs;
    }

    public List<StreamOffsetRange> getOffsetRanges() {
        return offsetRanges;
    }

    public long startOffset() {
        if (offsetRanges == null || offsetRanges.isEmpty()) {
            return S3StreamConstant.INVALID_OFFSET;
        }
        return offsetRanges.get(0).getStartOffset();
    }

    public long endOffset() {
        if (offsetRanges == null || offsetRanges.isEmpty()) {
            return S3StreamConstant.INVALID_OFFSET;
        }
        return offsetRanges.get(offsetRanges.size() - 1).getEndOffset();
    }

    public String toString() {
        return "S3ObjectMetadata(objectId=" + objectId + ", objectSize=" + objectSize + ", type=" + type + ", offsetRanges=" + offsetRanges
            + ", committedTimestamp=" + committedTimestamp + ", dataTimestamp=" + dataTimeInMs + ")";
    }

    public String key() {
        return ObjectUtils.genKey(0, "todocluster", objectId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3ObjectMetadata that = (S3ObjectMetadata) o;
        return objectId == that.objectId && orderId == that.orderId && objectSize == that.objectSize && committedTimestamp == that.committedTimestamp
            && dataTimeInMs == that.dataTimeInMs && type == that.type && offsetRanges.equals(that.offsetRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, orderId, objectSize, type, offsetRanges, committedTimestamp, dataTimeInMs);
    }
}
