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

package com.automq.stream.utils;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * Tracks in-flight requests and reports the oldest request that remains pending beyond a latency threshold.
 */
public final class PendingRequestTracker implements AutoCloseable {
    public static final long DEFAULT_LATENCY_THRESHOLD_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final long MIN_ROTATE_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final long latencyThresholdNanos;
    private final long rotateIntervalNanos;
    private final LongSupplier nanoTimeSupplier;
    private final ScheduledFuture<?> rotateTask;
    private volatile Set<Handle> active = ConcurrentHashMap.newKeySet();
    private volatile Set<Handle> cooling = ConcurrentHashMap.newKeySet();
    private volatile Set<Handle> overdue = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean rotating = new AtomicBoolean(false);

    public PendingRequestTracker() {
        this(DEFAULT_LATENCY_THRESHOLD_NANOS);
    }

    public PendingRequestTracker(long latencyThresholdNanos) {
        this(latencyThresholdNanos, System::nanoTime);
    }

    PendingRequestTracker(long latencyThresholdNanos, LongSupplier nanoTimeSupplier) {
        if (latencyThresholdNanos <= 0) {
            throw new IllegalArgumentException("latencyThresholdNanos must be positive");
        }
        this.latencyThresholdNanos = latencyThresholdNanos;
        this.rotateIntervalNanos = Math.max(latencyThresholdNanos / 2, MIN_ROTATE_INTERVAL_NANOS);
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.rotateTask = Threads.COMMON_SCHEDULER.scheduleWithFixedDelay(this::rotate, rotateIntervalNanos,
            rotateIntervalNanos, TimeUnit.NANOSECONDS);
    }

    public Handle begin() {
        Handle handle = new Handle(nanoTimeSupplier.getAsLong());
        active.add(handle);
        return handle;
    }

    public long pendingLatencyNanos() {
        long now = nanoTimeSupplier.getAsLong();
        long oldestStartNanos = Long.MAX_VALUE;
        for (Handle handle : overdue) {
            if (handle.done.get()) {
                overdue.remove(handle);
                continue;
            }
            long latencyNanos = now - handle.startNanos;
            if (latencyNanos >= latencyThresholdNanos) {
                oldestStartNanos = Math.min(oldestStartNanos, handle.startNanos);
            }
        }
        if (oldestStartNanos == Long.MAX_VALUE) {
            return 0;
        }
        return now - oldestStartNanos;
    }

    @Override
    public void close() {
        rotateTask.cancel(false);
        active.clear();
        cooling.clear();
        overdue.clear();
    }

    void rotate() {
        if (!rotating.compareAndSet(false, true)) {
            return;
        }
        try {
            overdue.removeIf(handle -> handle.done.get());
            // begin() may add to a stale bucket if it is paused across multiple rotations. This tracker accepts
            // that rare race because pending latency is only a stuck-request signal: the handle may enter overdue
            // one or more rotations later, while completed handles are still removed by close(), removeIf(), and
            // the post-add done check.
            Iterator<Handle> iterator = cooling.iterator();
            while (iterator.hasNext()) {
                Handle handle = iterator.next();
                overdue.add(handle);
                iterator.remove();
                if (handle.done.get()) {
                    overdue.remove(handle);
                }
            }
            Set<Handle> tmp = cooling;
            cooling = active;
            active = tmp;
        } finally {
            rotating.set(false);
        }
    }

    public class Handle implements AutoCloseable {
        private final long startNanos;
        private final AtomicBoolean done = new AtomicBoolean(false);

        private Handle(long startNanos) {
            this.startNanos = startNanos;
        }

        @Override
        public void close() {
            if (!done.compareAndSet(false, true)) {
                return;
            }
            active.remove(this);
            cooling.remove(this);
            overdue.remove(this);
        }
    }
}
