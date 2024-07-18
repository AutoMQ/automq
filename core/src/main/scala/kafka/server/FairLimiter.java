/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.server;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A fair limiter whose {@link #acquire} method is fair, i.e. the waiting threads are served in the order of arrival.
 */
public class FairLimiter implements Limiter {
    private final int maxPermits;
    /**
     * The lock used to protect @{link #acquireLocked}
     */
    private final Lock lock = new ReentrantLock(true);
    private final Semaphore permits;

    public FairLimiter(int size) {
        maxPermits = size;
        permits = new Semaphore(size);
    }

    @Override
    public Handler acquire(int permit) throws InterruptedException {
        lock.lock();
        try {
            permits.acquire(permit);
            return new FairHandler(permit);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Handler acquire(int permit, long timeoutMs) throws InterruptedException {
        long start = System.nanoTime();
        if (lock.tryLock(timeoutMs, TimeUnit.MILLISECONDS)) {
            try {
                // calculate the time left for {@code acquireLocked}
                long elapsed = System.nanoTime() - start;
                long left = TimeUnit.MILLISECONDS.toNanos(timeoutMs) - elapsed;
                // note: {@code left} may be negative here, but it's OK for acquireLocked
                return acquireLocked(permit, left);
            } finally {
                lock.unlock();
            }
        } else {
            // tryLock timeout, return null
            return null;
        }
    }

    @Override
    public int availablePermits() {
        return permits.availablePermits();
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
