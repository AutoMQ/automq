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

import java.util.Objects;

import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public class S3StreamObject {

    private final long objectId;
    private final long dataTimeInMs;
    private final StreamOffsetRange streamOffsetRange;

    public S3StreamObject(long objectId, long streamId, long startOffset, long endOffset, long dataTimeInMs) {
        this.objectId = objectId;
        this.streamOffsetRange = new StreamOffsetRange(streamId, startOffset, endOffset);
        this.dataTimeInMs = dataTimeInMs;
    }

    public StreamOffsetRange streamOffsetRange() {
        return streamOffsetRange;
    }

    public long objectId() {
        return objectId;
    }

    public S3ObjectType objectType() {
        return S3ObjectType.STREAM;
    }

    public long dataTimeInMs() {
        return dataTimeInMs;
    }

    public ApiMessageAndVersion toRecord() {
        return new ApiMessageAndVersion(new S3StreamObjectRecord()
            .setObjectId(objectId)
            .setStreamId(streamOffsetRange.getStreamId())
            .setStartOffset(streamOffsetRange.getStartOffset())
            .setEndOffset(streamOffsetRange.getEndOffset())
            .setDataTimeInMs(dataTimeInMs), (short) 0);
    }

    public static S3StreamObject of(S3StreamObjectRecord record) {
        S3StreamObject s3StreamObject = new S3StreamObject(
            record.objectId(), record.streamId(),
            record.startOffset(), record.endOffset(), record.dataTimeInMs());
        return s3StreamObject;
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
}
