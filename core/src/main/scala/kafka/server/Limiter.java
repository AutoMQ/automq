/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
