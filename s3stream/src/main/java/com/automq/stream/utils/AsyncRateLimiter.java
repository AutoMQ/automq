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

package com.automq.stream.utils;

import com.google.common.util.concurrent.RateLimiter;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnstableApiUsage")
public class AsyncRateLimiter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRateLimiter.class);
    private static final ScheduledExecutorService SCHEDULER = Threads.newSingleThreadScheduledExecutor("async-rate-limiter", true, LOGGER);
    private final Queue<Acquire> acquireQueue = new ConcurrentLinkedQueue<>();
    private final RateLimiter rateLimiter;
    private final ScheduledFuture<?> tickTask;
    private volatile boolean burst = false;
    private long burstBytes = 0L;

    public AsyncRateLimiter(double bytesPerSec) {
        rateLimiter = RateLimiter.create(bytesPerSec, 100, TimeUnit.MILLISECONDS);
        tickTask = SCHEDULER.scheduleAtFixedRate(this::tick, 1, 1, TimeUnit.MILLISECONDS);
    }

    public synchronized CompletableFuture<Void> acquire(int size) {
        if (acquireQueue.isEmpty() && rateLimiter.tryAcquire(size)) {
            if (burst) {
                burstBytes += size;
            }
            return CompletableFuture.completedFuture(null);
        } else {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            acquireQueue.add(new Acquire(cf, size));
            return cf;
        }
    }

    public synchronized void burst() {
        if (!burst) {
            burst = true;
            rateLimiter.setRate(Long.MAX_VALUE);
        }
    }

    public long getBurstBytes() {
        return this.burstBytes;
    }

    public void close() {
        tickTask.cancel(false);
    }

    private synchronized void tick() {
        for (; ; ) {
            Acquire acquire = acquireQueue.peek();
            if (acquire == null) {
                break;
            }
            if (burst || rateLimiter.tryAcquire(acquire.size)) {
                acquireQueue.poll();
                if (burst) {
                    burstBytes += acquire.size;
                }
                acquire.cf.complete(null);
            } else {
                break;
            }
        }
    }

    static final class Acquire {
        private final CompletableFuture<Void> cf;
        private final int size;

        Acquire(CompletableFuture<Void> cf, int size) {
            this.cf = cf;
            this.size = size;
        }

        public CompletableFuture<Void> cf() {
            return cf;
        }

        public int size() {
            return size;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (Acquire) obj;
            return Objects.equals(this.cf, that.cf) &&
                this.size == that.size;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cf, size);
        }

        @Override
        public String toString() {
            return "Acquire[" +
                "cf=" + cf + ", " +
                "size=" + size + ']';
        }
    }
}
