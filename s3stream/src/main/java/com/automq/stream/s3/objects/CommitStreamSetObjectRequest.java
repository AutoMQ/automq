/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.objects;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class CommitStreamSetObjectRequest {

    /**
     * The object id of the stream set object.
     */
    private long objectId;

    /**
     * The order id of the stream set object.
     * <p>
     * When the stream set object is generated by compacting, the order id is the first compacted object's order id.
     */
    private long orderId;

    /**
     * The real size of the stream set object in Storage.
     */
    private long objectSize;
    /**
     * The stream ranges of the stream set object.
     * <p>
     * The stream ranges are sorted by <code>[stream][epoch][startOffset]</code>
     */
    private List<ObjectStreamRange> streamRanges;

    /**
     * The stream objects which split from the stream set object.
     * <p>
     * The stream objects are sorted by <code>[stream][startOffset]</code>
     */
    private List<StreamObject> streamObjects;

    /**
     * The object ids which are compacted by the stream set object.
     */
    private List<Long> compactedObjectIds;

    public long getObjectId() {
        return objectId;
    }

    public void setObjectId(long objectId) {
        this.objectId = objectId;
    }

    public long getObjectSize() {
        return objectSize;
    }

    public void setObjectSize(long objectSize) {
        this.objectSize = objectSize;
    }

    public List<Long> getCompactedObjectIds() {
        if (compactedObjectIds == null) {
            return Collections.emptyList();
        }
        return compactedObjectIds;
    }

    public void setCompactedObjectIds(List<Long> compactedObjectIds) {
        this.compactedObjectIds = compactedObjectIds;
    }

    public List<ObjectStreamRange> getStreamRanges() {
        if (streamRanges == null) {
            return Collections.emptyList();
        }
        return streamRanges;
    }

    public void setStreamRanges(List<ObjectStreamRange> streamRanges) {
        this.streamRanges = streamRanges;
    }

    public void addStreamRange(ObjectStreamRange streamRange) {
        if (streamRanges == null) {
            streamRanges = new LinkedList<>();
        }
        streamRanges.add(streamRange);
    }

    public List<StreamObject> getStreamObjects() {
        if (streamObjects == null) {
            return Collections.emptyList();
        }
        return streamObjects;
    }

    public void setStreamObjects(List<StreamObject> streamObjects) {
        this.streamObjects = streamObjects;
    }

    public void addStreamObject(StreamObject streamObject) {
        if (streamObjects == null) {
            streamObjects = new LinkedList<>();
        }
        streamObjects.add(streamObject);
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    @Override
    public String toString() {
        return "CommitStreamSetObjectRequest{" +
            "objectId=" + objectId +
            ", orderId=" + orderId +
            ", objectSize=" + objectSize +
            ", streamRanges=" + streamRanges +
            ", streamObjects=" + streamObjects +
            ", compactedObjectIds=" + compactedObjectIds +
            '}';
    }
}