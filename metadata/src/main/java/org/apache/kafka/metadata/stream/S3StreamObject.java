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

import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;

import java.util.List;
import java.util.Objects;

public class S3StreamObject {

    private final long objectId;
    private final long streamId;
    private final long startOffset;
    private final long endOffset;

    public S3StreamObject(long objectId, long streamId, long startOffset, long endOffset) {
        this.objectId = objectId;
        this.streamId = streamId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public StreamOffsetRange streamOffsetRange() {
        return new StreamOffsetRange(streamId, startOffset, endOffset);
    }

    public long streamId() {
        return streamId;
    }

    public long startOffset() {
        return startOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    public long objectId() {
        return objectId;
    }

    public S3ObjectType objectType() {
        return S3ObjectType.STREAM;
    }

    public ApiMessageAndVersion toRecord(AutoMQVersion version) {
        return new ApiMessageAndVersion(new S3StreamObjectRecord()
            .setObjectId(objectId)
            .setStreamId(streamId)
            .setStartOffset(startOffset)
            .setEndOffset(endOffset), version.streamObjectRecordVersion());
    }

    public S3ObjectMetadata toMetadata() {
        return new S3ObjectMetadata(objectId, S3ObjectType.STREAM, List.of(streamOffsetRange()), 0L);
    }

    public static S3StreamObject of(S3StreamObjectRecord record) {
        return new S3StreamObject(record.objectId(), record.streamId(), record.startOffset(), record.endOffset());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3StreamObject that = (S3StreamObject) o;
        return objectId == that.objectId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId);
    }

    @Override
    public String toString() {
        return "S3StreamObject{" +
            "objectId=" + objectId +
            ", streamId=" + streamId +
            ", startOffset=" + startOffset +
            ", endOffset=" + endOffset +
            '}';
    }
}
