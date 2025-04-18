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

package com.automq.stream.s3.objects;

import com.automq.stream.s3.compact.CompactOperations;

import java.util.List;

public class CompactStreamObjectRequest {
    private long objectId;
    private long objectSize;
    private long streamId;
    private long startOffset;
    private long endOffset;
    private final long streamEpoch;
    /**
     * The generated object's attributes.
     */
    private final int attributes;
    /**
     * The source objects' id of the stream object.
     */
    private final List<Long> sourceObjectIds;
    private final List<CompactOperations> operations;

    public CompactStreamObjectRequest(
        long objectId,
        long objectSize,
        long streamId,
        long streamEpoch,
        long startOffset,
        long endOffset,
        List<Long> sourceObjectIds,
        List<CompactOperations> operations,
        int attributes) {
        this.objectId = objectId;
        this.objectSize = objectSize;
        this.streamId = streamId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.streamEpoch = streamEpoch;
        this.sourceObjectIds = sourceObjectIds;
        this.operations = operations;
        this.attributes = attributes;
    }

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

    public long getStreamId() {
        return streamId;
    }

    public void setStreamId(long streamId) {
        this.streamId = streamId;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public long getStreamEpoch() {
        return streamEpoch;
    }

    public List<Long> getSourceObjectIds() {
        return sourceObjectIds;
    }

    public List<CompactOperations> getOperations() {
        return operations;
    }

    public int getAttributes() {
        return attributes;
    }

    @Override
    public String toString() {
        return "CompactStreamObjectRequest{" +
            "objectId=" + objectId +
            ", objectSize=" + objectSize +
            ", streamId=" + streamId +
            ", startOffset=" + startOffset +
            ", endOffset=" + endOffset +
            ", streamEpoch=" + streamEpoch +
            ", attributes=" + attributes +
            ", sourceObjectIds=" + sourceObjectIds +
            ", operations=" + operations +
            '}';
    }
}
