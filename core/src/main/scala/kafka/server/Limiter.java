/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
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
     * Return the number of permits available.
     *
     * @return the number of permits available
     */
    int availablePermits();

    /**
     * A handler to release acquired permits.
     */
    interface Handler extends AutoCloseable {
    }
}
