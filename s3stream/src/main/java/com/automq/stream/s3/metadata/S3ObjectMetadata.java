/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.metadata;

import com.automq.stream.s3.objects.ObjectAttributes;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class S3ObjectMetadata {

    private final long objectId;

    /**
     * order id of the object.
     * <ul>
     *     <li> stream set object: order id of the stream set object.
     *     <li> STREAM object: meaningless.
     * </ul>
     */
    private final long orderId;
    private final S3ObjectType type;
    /**
     * stream offset ranges of the object.
     * <ul>
     *     <li> stream set object: one or more stream offset ranges.
     *     <li> STREAM object: only one stream offset range.
     * </ul>
     */
    private final List<StreamOffsetRange> offsetRanges;
    /**
     * logical timestamp in ms of the data in the object.
     */
    private final long dataTimeInMs;
    private long objectSize;
    /**
     * real committed timestamp of the data in the object.
     */
    private long committedTimestamp;
    /**
     * The object attributes {@link ObjectAttributes}.
     */
    private int attributes;

    // Only used for testing
    public S3ObjectMetadata(long objectId, long objectSize, S3ObjectType type) {
        this(objectId, type, Collections.emptyList(), S3StreamConstant.INVALID_TS, S3StreamConstant.INVALID_TS, objectSize,
            S3StreamConstant.INVALID_ORDER_ID);
    }

    public S3ObjectMetadata(long objectId, S3ObjectType type, List<StreamOffsetRange> offsetRanges, long dataTimeInMs) {
        this(objectId, type, offsetRanges, dataTimeInMs, S3StreamConstant.INVALID_TS, S3StreamConstant.INVALID_OBJECT_SIZE,
            S3StreamConstant.INVALID_ORDER_ID);
    }

    // Only used for testing
    public S3ObjectMetadata(
        long objectId, S3ObjectType type, List<StreamOffsetRange> offsetRanges, long dataTimeInMs,
        long committedTimestamp, long objectSize,
        long orderId) {
        this(objectId, type, offsetRanges, dataTimeInMs, committedTimestamp, objectSize, orderId, ObjectAttributes.DEFAULT.attributes());
    }

    public S3ObjectMetadata(long objectId, int attributes) {
        this(objectId, -1L, attributes);
    }

    public S3ObjectMetadata(long objectId, long objectSize, int attributes) {
        this(objectId, S3ObjectType.UNKNOWN, Collections.emptyList(), S3StreamConstant.INVALID_TS, S3StreamConstant.INVALID_TS, objectSize,
            S3StreamConstant.INVALID_ORDER_ID, attributes);
    }

    public S3ObjectMetadata(
        // these four params come from S3StreamSetObject or S3StreamObject
        long objectId, S3ObjectType type, List<StreamOffsetRange> offsetRanges, long dataTimeInMs,
        // these two params come from S3Object
        long committedTimestamp, long objectSize,
        // this param only comes from S3StreamSetObject
        long orderId, int attributes) {
        this.objectId = objectId;
        this.orderId = orderId;
        this.objectSize = objectSize;
        this.type = type;
        this.offsetRanges = offsetRanges;
        this.dataTimeInMs = dataTimeInMs;
        this.committedTimestamp = committedTimestamp;
        this.attributes = attributes;
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
        return offsetRanges.get(0).startOffset();
    }

    public long endOffset() {
        if (offsetRanges == null || offsetRanges.isEmpty()) {
            return S3StreamConstant.INVALID_OFFSET;
        }
        return offsetRanges.get(offsetRanges.size() - 1).endOffset();
    }

    public int attributes() {
        return attributes;
    }

    public void setAttributes(int attributes) {
        this.attributes = attributes;
    }

    public boolean intersect(long streamId, long startOffset, long endOffset) {
        if (offsetRanges == null || offsetRanges.isEmpty()) {
            return false;
        }
        for (StreamOffsetRange offsetRange : offsetRanges) {
            if (offsetRange.streamId() == streamId && offsetRange.intersect(startOffset, endOffset)) {
                return true;
            }
        }
        return false;
    }

    public String toString() {
        return "S3ObjectMetadata(objectId=" + objectId + ", objectSize=" + objectSize + ", type=" + type + ", offsetRanges=" + offsetRanges
            + ", committedTimestamp=" + committedTimestamp + ", dataTimestamp=" + dataTimeInMs + ")";
    }

    public String key() {
        return ObjectUtils.genKey(0, objectId);
    }

    public short bucket() {
        return ObjectAttributes.from(attributes).bucket();
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
