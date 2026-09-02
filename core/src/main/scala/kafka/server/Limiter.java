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

package kafka.server;

/**
 * Limits retained fetch memory and exposes the response lifecycle needed to release it.
 */
public interface Limiter {

    /**
     * Acquires permits, waiting until the request is admitted or its timeout expires.
     *
     * @param permits requested permits, which must not be negative
     * @param context acquisition timeout and owning connection
     * @return a permit handle that must be closed after use, or {@code null} when the acquisition times out
     * @throws InterruptedException if interrupted while waiting
     */
    Permit acquire(long permits, AcquireContext context) throws InterruptedException;

    /**
     * Executes a fetch task in the lane selected for its owning connection.
     *
     * @param connectionId connection that owns the task, or {@code null} for an internal fetch
     * @param task task to execute
     */
    void execute(String connectionId, Runnable task);

    /**
     * Carries request information needed for admission and connection classification.
     *
     * @param timeoutMs maximum wait time in milliseconds; a non-positive value means waiting indefinitely
     * @param connectionId connection that owns the acquired permit, or {@code null} for an internal fetch
     */
    record AcquireContext(long timeoutMs, String connectionId) {
    }

    /**
     * Owns permits granted by a limiter and tracks the retained fetch response lifecycle.
     */
    interface Permit extends AutoCloseable {

        /**
         * Marks the fetch response retaining this permit as ready to be sent.
         */
        void markResponseReady();

        /**
         * Releases permits until this handle owns the supplied amount.
         *
         * @param newPermits permits that should remain held
         * @return {@code true} if permits were released, or {@code false} if the value is outside the valid range
         */
        boolean releaseTo(long newPermits);

        /**
         * Returns the permits still held by this handle.
         */
        long permitsHeld();
    }
}
