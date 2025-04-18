/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package com.automq.stream.s3.index;

import com.google.common.base.Objects;

public class RangeIndex implements Comparable<RangeIndex> {
    public static final int OBJECT_SIZE = 3 * Long.BYTES + NodeRangeIndexCache.ZGC_OBJECT_HEADER_SIZE_BYTES;
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
