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

package kafka.server;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryLimiter {
    private final int maxPermits;
    /**
     * The lock used to protect @{link #acquireLocked}
     */
    private final Lock lock = new ReentrantLock(true);
    private final Semaphore permits;

    public MemoryLimiter(int size) {
        maxPermits = size;
        permits = new Semaphore(size);
    }

    /**
     * Acquire permits, if not enough, block until enough.
     * Note: this method is fair, which means the waiting threads will be served in order.
     *
     * @param permit the number of permits to acquire, should not be negative
     * @return a handler to release the permits
     */
    public Handler acquire(int permit) throws InterruptedException {
        lock.lock();
        try {
            permits.acquire(permit);
            return new Handler(permit);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Acquire permits, if not enough, block until enough or timeout.
     * Note: this method is fair, which means the waiting threads will be served in order.
     *
     * @param permit    the number of permits to acquire, should not be negative
     * @param timeoutMs the maximum time to wait for the permits, in milliseconds. A non-positive value means not to wait.
     * @return a handler to release the permits or null if timeout
     */
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

    private Handler acquireLocked(int permit, long timeoutNs) throws InterruptedException {
        if (permit > maxPermits) {
            permit = maxPermits;
        }
        boolean acquired = permits.tryAcquire(permit, timeoutNs, TimeUnit.NANOSECONDS);
        return acquired ? new Handler(permit) : null;
    }

    public class Handler implements AutoCloseable {
        private final int permit;

        public Handler(int permit) {
            this.permit = permit;
        }

        @Override
        public void close() {
            permits.release(permit);
        }
    }
}
