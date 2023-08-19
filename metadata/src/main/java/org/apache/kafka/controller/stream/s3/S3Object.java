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

package org.apache.kafka.controller.stream.s3;

import java.util.Optional;

/**
 * S3Object is the base class of object in S3.
 * Manages the lifecycle of S3Object.
 */
public abstract class S3Object implements Comparable<S3Object> {

    protected final Long objectId;

    protected Optional<Long> objectSize = Optional.empty();

    protected Optional<String> objectAddress = Optional.empty();

    protected Optional<Long> applyTimeInMs = Optional.empty();

    protected Optional<Long> createTimeInMs = Optional.empty();

    protected Optional<Long> destroyTimeInMs = Optional.empty();

    protected ObjectState objectState = ObjectState.UNINITIALIZED;

    protected S3ObjectType objectType = S3ObjectType.UNKNOWN;

    protected S3Object(final Long objectId) {
        this.objectId = objectId;
    }

    public void onApply() {
        if (this.objectState != ObjectState.UNINITIALIZED) {
            throw new IllegalStateException("Object is not in UNINITIALIZED state");
        }
        this.objectState = ObjectState.APPLIED;
        this.applyTimeInMs = Optional.of(System.currentTimeMillis());
    }

    public void onCreate(S3ObjectCreateContext createContext) {
        // TODO: decide fetch object metadata from S3 or let broker send it to controller
        if (this.objectState != ObjectState.APPLIED) {
            throw new IllegalStateException("Object is not in APPLIED state");
        }
        this.objectState = ObjectState.CREATED;
        this.createTimeInMs = Optional.of(createContext.createTimeInMs);
        this.objectSize = Optional.of(createContext.objectSize);
        this.objectAddress = Optional.of(createContext.objectAddress);
        this.objectType = createContext.objectType;
    }

    public void onDestroy() {
        if (this.objectState != ObjectState.CREATED) {
            throw new IllegalStateException("Object is not in CREATED state");
        }
        S3ObjectManager.destroy(this, () -> {
            this.objectState = ObjectState.DESTROYED;
            this.destroyTimeInMs = Optional.of(System.currentTimeMillis());
        });
    }

    public S3ObjectType getObjectType() {
        return objectType;
    }

    enum ObjectState {
        UNINITIALIZED,
        APPLIED,
        CREATED,
        MARK_DESTROYED,
        DESTROYED;
    }

    public class S3ObjectCreateContext {
        private final Long createTimeInMs;
        private final Long objectSize;
        private final String objectAddress;
        private final S3ObjectType objectType;
        public S3ObjectCreateContext(
            final Long createTimeInMs,
            final Long objectSize,
            final String objectAddress,
            final S3ObjectType objectType) {
            this.createTimeInMs = createTimeInMs;
            this.objectSize = objectSize;
            this.objectAddress = objectAddress;
            this.objectType = objectType;
        }
    }

    @Override
    public int compareTo(S3Object o) {
        return this.objectId.compareTo(o.objectId);
    }
}
