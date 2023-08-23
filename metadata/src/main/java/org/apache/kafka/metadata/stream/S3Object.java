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
import java.util.Optional;
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

    public void onApply() {
        if (this.s3ObjectState != S3ObjectState.UNINITIALIZED) {
            throw new IllegalStateException("Object is not in UNINITIALIZED state");
        }
        this.s3ObjectState = S3ObjectState.PREPARED;
        this.preparedTimeInMs = System.currentTimeMillis();
    }

    public void onCreate(S3ObjectCommitContext createContext) {
        // TODO: decide fetch object metadata from S3 or let broker send it to controller
        if (this.s3ObjectState != S3ObjectState.PREPARED) {
            throw new IllegalStateException("Object is not in APPLIED state");
        }
        this.s3ObjectState = S3ObjectState.COMMITTED;
        this.committedTimeInMs = createContext.committedTimeInMs;
        this.objectSize = createContext.objectSize;
        this.objectKey = createContext.objectAddress;
    }

    public void onMarkDestroy() {
        if (this.s3ObjectState != S3ObjectState.COMMITTED) {
            throw new IllegalStateException("Object is not in CREATED state");
        }
        this.s3ObjectState = S3ObjectState.MARK_DESTROYED;
    }

    public void onDestroy() {
        if (this.s3ObjectState != S3ObjectState.COMMITTED) {
            throw new IllegalStateException("Object is not in CREATED state");
        }
        // TODO: trigger destroy

    }

    public ApiMessageAndVersion toRecord() {
        return new ApiMessageAndVersion(new S3ObjectRecord()
            .setObjectId(objectId)
            .setObjectSize(objectSize)
            .setObjectState((byte) s3ObjectState.ordinal())
            .setPreparedTimeInMs(preparedTimeInMs)
            .setExpiredTimeInMs(expiredTimeInMs)
            .setCommittedTimeInMs(committedTimeInMs)
            .setDestroyedTimeInMs(destroyedTimeInMs), (short) 0);
    }

    static public class S3ObjectCommitContext {

        private final long committedTimeInMs;
        private final long objectSize;
        private final String objectAddress;

        public S3ObjectCommitContext(
            final long committedTimeInMs,
            final long objectSize,
            final String objectAddress) {
            this.committedTimeInMs = committedTimeInMs;
            this.objectSize = objectSize;
            this.objectAddress = objectAddress;
        }
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
