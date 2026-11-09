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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Paces request admission and actual byte acquisition on independent timelines. Registration only
 * determines when an SDK request may start; handles share the byte timeline without a concurrency cap.
 * Each grant advances its timeline by bytes/rate. Idle time does not accumulate credit, and only the
 * earliest pending deadline schedules a wakeup. Queued grants retain their planned deadlines so late
 * wakeups can catch up. Burst and close both permanently remove all waiting.
 */
public class AsyncRateLimiter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRateLimiter.class);
    private static final ScheduledExecutorService SCHEDULER = Threads.newSingleThreadScheduledExecutor("async-rate-limiter", true, LOGGER);
    private final Queue<Registration> registrations = new ArrayDeque<>();
    private final Queue<Acquire> acquires = new ArrayDeque<>();
    private final double bytesPerSec;
    private final Time time;
    private final ScheduledExecutorService scheduler;
    private long nextStartNanos;
    private long nextAcquireNanos;
    private boolean unlimited;
    private ScheduledFuture<?> wakeup;
    private long wakeupAtNanos;
    private long wakeupVersion;

    public AsyncRateLimiter(double bytesPerSec) {
        this(bytesPerSec, Time.SYSTEM, SCHEDULER);
    }

    AsyncRateLimiter(double bytesPerSec, Time time) {
        this(bytesPerSec, time, SCHEDULER);
    }

    AsyncRateLimiter(double bytesPerSec, Time time, ScheduledExecutorService scheduler) {
        if (!(bytesPerSec > 0)) {
            throw new IllegalArgumentException("bytesPerSec must be positive");
        }
        this.bytesPerSec = bytesPerSec;
        this.time = time;
        this.scheduler = scheduler;
        nextStartNanos = time.nanoseconds();
        nextAcquireNanos = nextStartNanos;
    }

    /** Schedules request admission without consuming the bytes that its handle will acquire. */
    public synchronized CompletableFuture<Handle> register(long requestBytes) {
        if (unlimited) {
            return CompletableFuture.completedFuture(new Handle());
        }
        if (requestBytes < 0) {
            throw new IllegalArgumentException("requestBytes must not be negative");
        }
        long now = time.nanoseconds();
        if (registrations.isEmpty() && now - nextStartNanos >= 0) {
            nextStartNanos = now + durationNanos(requestBytes);
            return CompletableFuture.completedFuture(new Handle());
        }
        CompletableFuture<Handle> future = new CompletableFuture<>();
        registrations.add(new Registration(requestBytes, future));
        schedule(now);
        return future;
    }

    /** An admitted SDK request whose chunks share the task's byte timeline. */
    public final class Handle {
        /** Acquires this chunk's bytes from the shared task limiter. */
        public CompletableFuture<Void> acquire(int bytes) {
            return AsyncRateLimiter.this.acquire(bytes);
        }
    }

    /** Grants bytes in arrival order; a grant reserves the time needed before the next grant. */
    public synchronized CompletableFuture<Void> acquire(int bytes) {
        if (unlimited) {
            return CompletableFuture.completedFuture(null);
        }
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be positive");
        }
        long now = time.nanoseconds();
        if (acquires.isEmpty() && now - nextAcquireNanos >= 0) {
            nextAcquireNanos = now + durationNanos(bytes);
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        acquires.add(new Acquire(bytes, future));
        schedule(now);
        return future;
    }

    /** Permanently removes task throttling, including pending registrations and chunk acquisitions. */
    public void burst() {
        List<Runnable> completions = new ArrayList<>();
        synchronized (this) {
            unlimited = true;
            cancelWakeup();
            drain(time.nanoseconds(), completions);
        }
        completions.forEach(Runnable::run);
    }

    /** Stops scheduling and passes all pending and future operations through. */
    public void close() {
        burst();
    }

    private long durationNanos(long bytes) {
        return (long) Math.min(Long.MAX_VALUE / 2.0, Math.ceil(bytes / bytesPerSec * 1_000_000_000.0));
    }

    private void drain(long now, List<Runnable> completions) {
        while (!registrations.isEmpty() && (unlimited || now - nextStartNanos >= 0)) {
            Registration registration = registrations.poll();
            nextStartNanos += durationNanos(registration.bytes);
            completions.add(() -> registration.future.complete(new Handle()));
        }
        while (!acquires.isEmpty() && (unlimited || now - nextAcquireNanos >= 0)) {
            Acquire acquire = acquires.poll();
            nextAcquireNanos += durationNanos(acquire.bytes);
            completions.add(() -> acquire.future.complete(null));
        }
    }

    private void schedule(long now) {
        if (unlimited || (registrations.isEmpty() && acquires.isEmpty())) {
            return;
        }
        long delay = registrations.isEmpty() ? Long.MAX_VALUE : Math.max(0, nextStartNanos - now);
        if (!acquires.isEmpty()) {
            delay = Math.min(delay, Math.max(0, nextAcquireNanos - now));
        }
        if (wakeup != null && wakeupAtNanos - now <= delay) {
            return;
        }
        cancelWakeup();
        wakeupAtNanos = now + delay;
        long version = wakeupVersion;
        wakeup = scheduler.schedule(() -> wakeUp(version), delay, TimeUnit.NANOSECONDS);
    }

    private void cancelWakeup() {
        wakeupVersion++;
        if (wakeup != null) {
            wakeup.cancel(false);
            wakeup = null;
        }
    }

    private void wakeUp(long version) {
        List<Runnable> completions = new ArrayList<>();
        synchronized (this) {
            // A cancelled callback may already be running; it must not disturb its replacement.
            if (version != wakeupVersion) {
                return;
            }
            cancelWakeup();
            long now = time.nanoseconds();
            drain(now, completions);
            schedule(now);
        }
        // Callers can reenter the limiter or acquire body monitors from these callbacks.
        completions.forEach(Runnable::run);
    }

    // Advances a manually controlled clock's pending work in deterministic tests.
    void tick() {
        long version;
        synchronized (this) {
            version = wakeupVersion;
        }
        wakeUp(version);
    }

    private record Registration(long bytes, CompletableFuture<Handle> future) { }

    private record Acquire(int bytes, CompletableFuture<Void> future) { }
}
