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

package com.automq.stream.s3.operator;

import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.threads.EventLoop;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.util.HashedWheelTimer;

/**
 * Schedules speculative requests without coupling scheduling to read or write semantics.
 * Owns pending tasks until execution starts, and runs at most one retry at a time.
 */
final class FastRetryManager implements AutoCloseable {
    private static final int MAX_INFLIGHT_RETRY_COUNT = 1;
    private static final int MAX_PENDING_RETRY_COUNT = 4096;
    private final HashedWheelTimer timer;
    private final EventLoop worker;
    private final Queue<FastRetryTask> tasks = new ArrayBlockingQueue<>(MAX_PENDING_RETRY_COUNT);
    private final Set<FastRetryTask> pendingTimers = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean workScheduled = new AtomicBoolean(false);
    private final Semaphore permit = new Semaphore(MAX_INFLIGHT_RETRY_COUNT);
    private volatile boolean closed;

    FastRetryManager(String threadPrefix) {
        timer = new HashedWheelTimer(
            ThreadUtils.createThreadFactory(threadPrefix + "fast-retry-timer", true), 10, TimeUnit.MILLISECONDS, 1000);
        worker = new EventLoop(threadPrefix + "fast-retry-worker");
    }

    synchronized void schedule(FastRetryTask task, long delayMillis) {
        if (closed) {
            task.discard();
            return;
        }
        pendingTimers.add(task);
        try {
            timer.newTimeout(timeout -> {
                pendingTimers.remove(task);
                if (closed || task.isDone()) {
                    task.discard();
                } else {
                    task.markEnqueued();
                    if (tasks.offer(task)) {
                        submitWork();
                    } else {
                        task.discard();
                    }
                }
            }, delayMillis, TimeUnit.MILLISECONDS);
        } catch (RuntimeException e) {
            pendingTimers.remove(task);
            task.discard();
            throw e;
        }
    }

    private void submitWork() {
        if (closed) {
            clearTasks();
            return;
        }
        if (!workScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            worker.execute(this::doWork);
        } catch (IllegalStateException e) {
            workScheduled.set(false);
            clearTasks();
        }
    }

    private void doWork() {
        try {
            while (!closed && permit.tryAcquire()) {
                FastRetryTask task = tasks.poll();
                if (task == null) {
                    permit.release();
                    break;
                }
                if (closed || task.isDone()) {
                    permit.release();
                    task.discard();
                    continue;
                }
                CompletableFuture<Void> retryCf;
                try {
                    retryCf = task.execute();
                } catch (Throwable e) {
                    task.discard();
                    retryCf = CompletableFuture.failedFuture(e);
                }
                retryCf.whenComplete((nil, ex) -> {
                    permit.release();
                    submitWork();
                });
            }
        } finally {
            workScheduled.set(false);
            if (!tasks.isEmpty() && (closed || permit.availablePermits() > 0)) {
                submitWork();
            }
        }
    }

    private void clearTasks() {
        FastRetryTask task;
        while ((task = tasks.poll()) != null) {
            task.discard();
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        timer.stop();
        pendingTimers.forEach(FastRetryTask::discard);
        pendingTimers.clear();
        clearTasks();
        worker.shutdownGracefully();
    }
}
