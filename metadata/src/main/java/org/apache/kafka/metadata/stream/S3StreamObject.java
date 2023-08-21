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

import java.util.Optional;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public class S3StreamObject extends S3Object {

    private S3ObjectStreamIndex streamIndex;

    public S3StreamObject(final Long objectId) {
        super(objectId);
    }

    @Override
    public void onCreate(S3ObjectCommitContext createContext) {
        super.onCreate(createContext);
        if (!(createContext instanceof StreamObjectCommitContext)) {
            throw new IllegalArgumentException();
        }
        this.streamIndex = ((StreamObjectCommitContext) createContext).streamIndex;
    }

    @Override
    public int compareTo(S3Object o) {
        if (!(o instanceof S3StreamObject)) {
            throw new IllegalArgumentException("Cannot compare StreamObject with non-StreamObject");
        }
        S3StreamObject s3StreamObject = (S3StreamObject) o;
        // order by streamId first, then startOffset
        int res = this.streamIndex.getStreamId().compareTo(s3StreamObject.streamIndex.getStreamId());
        return res == 0 ? this.streamIndex.getStartOffset().compareTo(s3StreamObject.streamIndex.getStartOffset()) : res;
    }

    class StreamObjectCommitContext extends S3ObjectCommitContext {

        private final S3ObjectStreamIndex streamIndex;

        public StreamObjectCommitContext(
            final Long createTimeInMs,
            final Long objectSize,
            final String objectAddress,
            final S3ObjectType objectType,
            final S3ObjectStreamIndex streamIndex) {
            super(createTimeInMs, objectSize, objectAddress, objectType);
            this.streamIndex = streamIndex;
        }
    }

    public S3ObjectStreamIndex getStreamIndex() {
        return streamIndex;
    }

    public ApiMessageAndVersion toRecord() {
        return new ApiMessageAndVersion(new S3StreamObjectRecord()
            .setObjectId(objectId)
            .setStreamId(streamIndex.getStreamId())
            .setObjectState((byte) s3ObjectState.ordinal())
            .setObjectType((byte) objectType.ordinal())
            .setAppliedTimeInMs(appliedTimeInMs.get())
            .setExpiredTimeInMs(expiredTimeInMs.get())
            .setCommittedTimeInMs(committedTimeInMs.get())
            .setDestroyedTimeInMs(destroyedTimeInMs.get())
            .setObjectSize(objectSize.get())
            .setStartOffset(streamIndex.getStartOffset())
            .setEndOffset(streamIndex.getEndOffset()), (short) 0);
    }

    public static S3StreamObject of(S3StreamObjectRecord record) {
        S3StreamObject s3StreamObject = new S3StreamObject(record.objectId());
        s3StreamObject.objectType = S3ObjectType.fromByte(record.objectType());
        s3StreamObject.s3ObjectState = S3ObjectState.fromByte(record.objectState());
        s3StreamObject.appliedTimeInMs = Optional.of(record.appliedTimeInMs());
        s3StreamObject.expiredTimeInMs = Optional.of(record.expiredTimeInMs());
        s3StreamObject.committedTimeInMs = Optional.of(record.committedTimeInMs());
        s3StreamObject.destroyedTimeInMs = Optional.of(record.destroyedTimeInMs());
        s3StreamObject.objectSize = Optional.of(record.objectSize());
        s3StreamObject.streamIndex = new S3ObjectStreamIndex(record.streamId(), record.startOffset(), record.endOffset());
        return s3StreamObject;
    }
}
