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

package com.automq.stream.s3.operator;

import com.ibm.asyncutil.locks.AsyncSemaphore;
import com.ibm.asyncutil.locks.FairAsyncSemaphore;

import java.util.concurrent.CompletableFuture;

/**
 * A limiter that uses an async semaphore to limit the volume of network traffic.
 */
public class TrafficVolumeLimiter {

    private static final long MAX_SEMAPHORE_PERMITS = FairAsyncSemaphore.MAX_PERMITS;

    /**
     * The semaphore used to limit the volume of network traffic in bytes.
     */
    private final AsyncSemaphore semaphore;

    /**
     * The current volume of network traffic in bytes.
     */
    private long currentVolume;

    /**
     * Create a limiter without limiting.
     */
    public TrafficVolumeLimiter() {
        this(MAX_SEMAPHORE_PERMITS);
    }

    public TrafficVolumeLimiter(long bytes) {
        this.semaphore = new FairAsyncSemaphore(bytes);
        this.currentVolume = bytes;
    }

    public long currentVolume() {
        return currentVolume;
    }

    /**
     * Update the current volume of network traffic.
     * Note: this method is not thread-safe.
     */
    public void update(long bytes) {
        if (bytes > currentVolume) {
            semaphore.release(bytes - currentVolume);
        } else {
            semaphore.acquire(currentVolume - bytes);
        }
        currentVolume = bytes;
    }

    /**
     * Consume the specified number of bytes and return a CompletableFuture that will be completed when the bytes are consumed.
     * Note: DO NOT perform any heavy operations in the callback, otherwise it will block thread which calls {@link #release}
     */
    public CompletableFuture<Void> acquire(long bytes) {
        return semaphore.acquire(bytes).toCompletableFuture();
    }

    /**
     * Release the specified number of bytes.
     * It may complete a number of waiting futures returned by {@link #acquire} and execute their callbacks.
     */
    public void release(long bytes) {
        semaphore.release(bytes);
    }
}
