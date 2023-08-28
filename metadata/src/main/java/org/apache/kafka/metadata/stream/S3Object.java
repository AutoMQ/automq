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
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

/**
 * S3Object is the base class of object in S3. Manages the lifecycle of S3Object.
 */
public class S3Object implements Comparable<S3Object> {

    private final long objectId;

    private long objectSize = -1;

    private String objectKey;

    /**
     * The time when broker ask for preparing the object
     */
    private long preparedTimeInMs;

    /**
     * The time when the object will be expired if it is not be committed
     */
    private long expiredTimeInMs;

    /**
     * The time when the object is committed
     */
    private long committedTimeInMs;

    /**
     * The time when the object is destroyed
     */
    private long destroyedTimeInMs;

    private S3ObjectState s3ObjectState = S3ObjectState.UNINITIALIZED;

    private S3Object(final long objectId) {
        this.objectId = objectId;
    }

    public S3Object(
        final long objectId,
        final long objectSize,
        final String objectKey,
        final long preparedTimeInMs,
        final long expiredTimeInMs,
        final long committedTimeInMs,
        final long destroyedTimeInMs,
        final S3ObjectState s3ObjectState) {
        this.objectId = objectId;
        this.objectSize = objectSize;
        this.objectKey = objectKey;
        this.preparedTimeInMs = preparedTimeInMs;
        this.expiredTimeInMs = expiredTimeInMs;
        this.committedTimeInMs = committedTimeInMs;
        this.destroyedTimeInMs = destroyedTimeInMs;
        this.s3ObjectState = s3ObjectState;
    }

    public ApiMessageAndVersion toRecord() {
        return new ApiMessageAndVersion(new S3ObjectRecord()
            .setObjectId(objectId)
            .setObjectSize(objectSize)
            .setObjectState(s3ObjectState.toByte())
            .setPreparedTimeInMs(preparedTimeInMs)
            .setExpiredTimeInMs(expiredTimeInMs)
            .setCommittedTimeInMs(committedTimeInMs)
            .setDestroyedTimeInMs(destroyedTimeInMs), (short) 0);
    }

    public static S3Object of(S3ObjectRecord record) {
        return new S3Object(
            record.objectId(), record.objectSize(), null,
            record.preparedTimeInMs(), record.expiredTimeInMs(), record.committedTimeInMs(), record.destroyedTimeInMs(),
            S3ObjectState.fromByte(record.objectState()));
    }

    @Override
    public int compareTo(S3Object o) {
        return Long.compare(this.objectId, o.objectId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3Object s3Object = (S3Object) o;
        return Objects.equals(objectId, s3Object.objectId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId);
    }

    public long getObjectId() {
        return objectId;
    }

    public long getObjectSize() {
        return objectSize;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public long getPreparedTimeInMs() {
        return preparedTimeInMs;
    }

    public long getCommittedTimeInMs() {
        return committedTimeInMs;
    }

    public long getDestroyedTimeInMs() {
        return destroyedTimeInMs;
    }

    public long getExpiredTimeInMs() {
        return expiredTimeInMs;
    }

    public S3ObjectState getS3ObjectState() {
        return s3ObjectState;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > expiredTimeInMs;
    }
}
