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

/**
 * S3Object is the base class of object in S3. Manages the lifecycle of S3Object.
 */
public abstract class S3Object implements Comparable<S3Object> {

    protected final Long objectId;

    protected Optional<Long> objectSize = Optional.empty();

    protected Optional<String> objectAddress = Optional.empty();

    /**
     * The time when broker apply the object
     */
    protected Optional<Long> appliedTimeInMs = Optional.empty();

    /**
     * The time when the object will be expired if it is not be committed
     */
    protected Optional<Long> expiredTimeInMs = Optional.empty();

    /**
     * The time when the object is committed
     */
    protected Optional<Long> committedTimeInMs = Optional.empty();

    /**
     * The time when the object is destroyed
     */
    protected Optional<Long> destroyedTimeInMs = Optional.empty();

    protected S3ObjectState s3ObjectState = S3ObjectState.UNINITIALIZED;

    protected S3ObjectType objectType = S3ObjectType.UNKNOWN;

    protected S3Object(final Long objectId) {
        this.objectId = objectId;
    }

    protected S3Object(
        final Long objectId,
        final Long objectSize,
        final String objectAddress,
        final Long appliedTimeInMs,
        final Long expiredTimeInMs,
        final Long committedTimeInMs,
        final Long destroyedTimeInMs,
        final S3ObjectState s3ObjectState,
        final S3ObjectType objectType) {
        this.objectId = objectId;
        this.objectSize = Optional.of(objectSize);
        this.objectAddress = Optional.of(objectAddress);
        this.appliedTimeInMs = Optional.of(appliedTimeInMs);
        this.expiredTimeInMs = Optional.of(expiredTimeInMs);
        this.committedTimeInMs = Optional.of(committedTimeInMs);
        this.destroyedTimeInMs = Optional.of(destroyedTimeInMs);
        this.objectType = objectType;
        this.s3ObjectState = s3ObjectState;
    }

    public void onApply() {
        if (this.s3ObjectState != S3ObjectState.UNINITIALIZED) {
            throw new IllegalStateException("Object is not in UNINITIALIZED state");
        }
        this.s3ObjectState = S3ObjectState.APPLIED;
        this.appliedTimeInMs = Optional.of(System.currentTimeMillis());
    }

    public void onCreate(S3ObjectCommitContext createContext) {
        // TODO: decide fetch object metadata from S3 or let broker send it to controller
        if (this.s3ObjectState != S3ObjectState.APPLIED) {
            throw new IllegalStateException("Object is not in APPLIED state");
        }
        this.s3ObjectState = S3ObjectState.CREATED;
        this.committedTimeInMs = Optional.of(createContext.committedTimeInMs);
        this.objectSize = Optional.of(createContext.objectSize);
        this.objectAddress = Optional.of(createContext.objectAddress);
        this.objectType = createContext.objectType;
    }

    public void onMarkDestroy() {
        if (this.s3ObjectState != S3ObjectState.CREATED) {
            throw new IllegalStateException("Object is not in CREATED state");
        }
        this.s3ObjectState = S3ObjectState.MARK_DESTROYED;
    }

    public void onDestroy() {
        if (this.s3ObjectState != S3ObjectState.CREATED) {
            throw new IllegalStateException("Object is not in CREATED state");
        }
        // TODO: trigger destroy

    }

    public S3ObjectType getObjectType() {
        return objectType;
    }

    public class S3ObjectCommitContext {

        private final Long committedTimeInMs;
        private final Long objectSize;
        private final String objectAddress;
        private final S3ObjectType objectType;

        public S3ObjectCommitContext(
            final Long committedTimeInMs,
            final Long objectSize,
            final String objectAddress,
            final S3ObjectType objectType) {
            this.committedTimeInMs = committedTimeInMs;
            this.objectSize = objectSize;
            this.objectAddress = objectAddress;
            this.objectType = objectType;
        }
    }

    @Override
    public int compareTo(S3Object o) {
        return this.objectId.compareTo(o.objectId);
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

    public Long getObjectId() {
        return objectId;
    }

    public Optional<Long> getObjectSize() {
        return objectSize;
    }

    public Optional<String> getObjectAddress() {
        return objectAddress;
    }

    public Optional<Long> getAppliedTimeInMs() {
        return appliedTimeInMs;
    }

    public Optional<Long> getCommittedTimeInMs() {
        return committedTimeInMs;
    }

    public Optional<Long> getDestroyedTimeInMs() {
        return destroyedTimeInMs;
    }

    public S3ObjectState getS3ObjectState() {
        return s3ObjectState;
    }
}
