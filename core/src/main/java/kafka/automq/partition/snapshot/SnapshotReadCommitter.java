/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.automq.partition.snapshot;

import org.apache.kafka.common.utils.Time;

import com.automq.stream.s3.ConfirmWAL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Coalesces cache-pressure commits across this broker's snapshot sessions. The manager owns the executor;
 * this helper creates no threads and retains at most one running batch and one pending batch.
 * Mutable state is protected by this monitor, but WAL calls and future completion run outside it.
 * Requests arriving after a commit starts must join the next batch: the running upload need not cover their data.
 */
final class SnapshotReadCommitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotReadCommitter.class);
    static final long MIN_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);
    private final Time time;
    private final ScheduledExecutorService executor;
    private final ConfirmWAL confirmWAL;
    private CompletableFuture<Void> pending;
    private boolean running;
    private boolean started;
    private long lastStartNs;

    SnapshotReadCommitter(Time time, ScheduledExecutorService executor, ConfirmWAL confirmWAL) {
        this.time = time;
        this.executor = executor;
        this.confirmWAL = confirmWAL;
    }

    /** Enqueues a commit covering data visible when requested; never completes it from an older in-flight upload. */
    synchronized CompletableFuture<Void> request() {
        if (pending == null) {
            pending = new CompletableFuture<>();
        }
        if (!running) {
            schedule();
        }
        return pending;
    }

    // Called under the monitor. The manager executor lives for the broker lifetime.
    private void schedule() {
        long delayNs = started ? Math.max(0, MIN_INTERVAL_NS - (time.nanoseconds() - lastStartNs)) : 0;
        executor.schedule(this::commit, delayNs, TimeUnit.NANOSECONDS);
        running = true;
    }

    private void commit() {
        CompletableFuture<Void> batch;
        synchronized (this) {
            batch = pending;
            pending = null;
            started = true;
            lastStartNs = time.nanoseconds();
        }
        CompletableFuture<Void> commitCf;
        try {
            // Zero linger selects force-upload: bypass only the upload task's adaptive pacing.
            // Existing object-storage admission and the commit-before-read barrier still apply.
            commitCf = confirmWAL.commit(0, false);
        } catch (Exception ex) {
            commitCf = new CompletableFuture<>();
            commitCf.completeExceptionally(ex);
        }
        commitCf.whenComplete((nil, ex) -> {
            if (ex == null) {
                batch.complete(null);
            } else {
                LOGGER.warn("Snapshot cache-pressure commit failed", ex);
                batch.completeExceptionally(ex);
            }
            synchronized (SnapshotReadCommitter.this) {
                running = false;
                if (pending != null) {
                    schedule();
                }
            }
        });
    }
}
