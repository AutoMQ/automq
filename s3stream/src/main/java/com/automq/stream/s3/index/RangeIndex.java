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

package com.automq.stream.s3.index;

import com.google.common.base.Objects;

public class RangeIndex implements Comparable<RangeIndex> {
    public static final int SIZE = 3 * Long.BYTES + NodeRangeIndexCache.ZGC_OBJECT_HEADER_SIZE_BYTES;
    private final long startOffset;
    private final long endOffset;
    private final long objectId;

    public RangeIndex(long startOffset, long endOffset, long objectId) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.objectId = objectId;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public long getObjectId() {
        return objectId;
    }

    @Override
    public int compareTo(RangeIndex o) {
        return Long.compare(this.startOffset, o.startOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        RangeIndex index = (RangeIndex) o;
        return startOffset == index.startOffset && endOffset == index.endOffset && objectId == index.objectId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(startOffset, endOffset, objectId);
    }

    @Override
    public String toString() {
        return "RangeIndex{" +
            "startOffset=" + startOffset +
            ", endOffset=" + endOffset +
            ", objectId=" + objectId +
            '}';
    }
}
