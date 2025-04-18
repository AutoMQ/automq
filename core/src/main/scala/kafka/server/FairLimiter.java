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

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

    /**
     * The name of this limiter, used for metrics.
     */
    private final String name;
    /**
     * The number of threads waiting for permits, used for metrics.
     */
    private final AtomicInteger waitingThreads = new AtomicInteger(0);

    public FairLimiter(int size, String name) {
        this.maxPermits = size;
        this.permits = new Semaphore(size);
        this.name = name;
    }

    @Override
    public Handler acquire(int permit) throws InterruptedException {
        waitingThreads.incrementAndGet();
        try {
            return acquire0(permit);
        } finally {
            waitingThreads.decrementAndGet();
        }
    }

    private Handler acquire0(int permit) throws InterruptedException {
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
        waitingThreads.incrementAndGet();
        try {
            return acquire0(permit, timeoutMs);
        } finally {
            waitingThreads.decrementAndGet();
        }
    }

    private Handler acquire0(int permit, long timeoutMs) throws InterruptedException {
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
    public int maxPermits() {
        return maxPermits;
    }

    @Override
    public int availablePermits() {
        return permits.availablePermits();
    }

    @Override
    public int waitingThreads() {
        return waitingThreads.get();
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
