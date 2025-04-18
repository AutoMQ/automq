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
 * A limiter that limits the number of permits that can be acquired at a time.
 */
public interface Limiter {

    /**
     * Acquire permits, if not enough, block until enough.
     *
     * @param permit the number of permits to acquire, should not be negative
     * @return a handler to release the permits, never null. The handler should be closed after use.
     * @throws InterruptedException if interrupted while waiting
     */
    Handler acquire(int permit) throws InterruptedException;

    /**
     * Acquire permits, if not enough, block until enough or timeout.
     *
     * @param permit    the number of permits to acquire, should not be negative
     * @param timeoutMs the maximum time to wait for the permits, in milliseconds. A non-positive value means not to wait.
     * @return a handler to release the permits or null if timeout. If not null, the handler should be closed after use.
     * @throws InterruptedException if interrupted while waiting
     */
    Handler acquire(int permit, long timeoutMs) throws InterruptedException;

    /**
     * Return the maximum number of permits that can be acquired at a time.
     *
     * @return the maximum number of permits that can be acquired at a time
     */
    int maxPermits();

    /**
     * Return the number of permits available.
     *
     * @return the number of permits available
     */
    int availablePermits();

    /**
     * Return the number of threads waiting for permits.
     */
    int waitingThreads();

    /**
     * Return the name of this limiter.
     */
    String name();

    /**
     * A handler to release acquired permits.
     */
    interface Handler extends AutoCloseable {

        /**
         * Release part of the acquired permits.
         *
         * @param permits the number of permits to release, should not be negative or greater than the permits held
         *                by this handler
         * @throws IllegalArgumentException if the permits is negative or greater than the permits held by this handler
         */
        void release(int permits);

        /**
         * Release part of the acquired permits to a new number of permits.
         *
         * @param newPermits the new number of permits, should not be negative or greater than the permits held
         *                   by this handler
         * @return true if the permits are released to the new number, false if the new number is invalid
         */
        default boolean releaseTo(int newPermits) {
            int held = permitsHeld();
            if (newPermits < 0 || newPermits > held) {
                return false;
            }
            release(held - newPermits);
            return true;
        }

        /**
         * Return the number of permits held by this handler.
         *
         * @return the number of permits held by this handler
         */
        int permitsHeld();
    }
}
