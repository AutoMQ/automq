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

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * A fair limiter whose {@link #acquire} method is fair, i.e. the waiting threads are served in the order of arrival.
 */
public class FairLimiter implements Limiter {
    private final int maxPermits;
    private final Semaphore permits;

    /**
     * The name of this limiter, used for metrics.
     */
    private final String name;

    public FairLimiter(int size, String name) {
        this.maxPermits = size;
        this.permits = new Semaphore(size, true);
        this.name = name;
    }

    @Override
    public Handler acquire(int permit) throws InterruptedException {
        permits.acquire(permit);
        return new FairHandler(permit);
    }

    @Override
    public Handler acquire(int permit, long timeoutMs) throws InterruptedException {
        return acquireLocked(permit, timeoutMs);
    }

    @Override
    public int maxPermits() {
        return maxPermits;
    }

    @Override
    public int availablePermits() {
        return permits.availablePermits();
    }

    @Override
    public int waitingThreads() {
        return permits.getQueueLength();
    }

    @Override
    public String name() {
        return name;
    }

    private Handler acquireLocked(int permit, long timeoutNs) throws InterruptedException {
        if (permit > maxPermits) {
            permit = maxPermits;
        }
        boolean acquired = permits.tryAcquire(permit, timeoutNs, TimeUnit.NANOSECONDS);
        return acquired ? new FairHandler(permit) : null;
    }

    public class FairHandler implements Handler {
        private int permit;

        public FairHandler(int permit) {
            this.permit = permit;
        }

        @Override
        public void release(int p) {
            if (p < 0) {
                throw new IllegalArgumentException(String.format("The number of permits to release (%d) should not be negative", p));
            }
            if (p > permit) {
                throw new IllegalArgumentException(String.format("The number of permits to release (%d) should not be greater than the permits held by this handler (%d)", p, permit));
            }
            permits.release(p);
            permit -= p;
        }

        @Override
        public int permitsHeld() {
            return permit;
        }

        @Override
        public void close() {
            permits.release(permit);
        }
    }
}
