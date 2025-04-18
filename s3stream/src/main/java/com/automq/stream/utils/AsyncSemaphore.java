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

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public class AsyncSemaphore {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSemaphore.class);
    private final Queue<AsyncSemaphoreTask> tasks = new LinkedList<>();
    private long permits;

    public AsyncSemaphore(long permits) {
        this.permits = permits;
    }

    /**
     * Acquire permits, if permits are not enough, the task will be added to the queue
     *
     * @param requiredPermits the required permits
     * @param task            task to run when the permits are available, the task should return a CompletableFuture
     *                        which will be completed when the permits could be released.
     * @param executor       the executor to run the task when the permits are available
     * @return true if the permits are acquired, false if the task is added to the waiting queue.
     */
    public synchronized boolean acquire(long requiredPermits, Supplier<CompletableFuture<?>> task,
        Executor executor) {
        if (permits >= 0) {
            // allow permits minus to negative
            permits -= requiredPermits;
            try {
                task.get().whenComplete((nil, ex) -> release(requiredPermits));
            } catch (Throwable e) {
                LOGGER.error("Error in task", e);
                release(requiredPermits);
            }
            return true;
        } else {
            tasks.add(new AsyncSemaphoreTask(requiredPermits, task, executor));
            return false;
        }
    }

    public synchronized boolean requiredRelease() {
        return permits <= 0 || !tasks.isEmpty();
    }

    public synchronized long permits() {
        return permits;
    }

    synchronized void release(long requiredPermits) {
        permits += requiredPermits;
        if (permits > 0) {
            AsyncSemaphoreTask t = tasks.poll();
            if (t != null) {
                // use executor to reset the thread stack to avoid stack overflow
                t.executor.execute(() -> acquire(t.requiredPermits, t.task, t.executor));
            }
        }
    }

    static class AsyncSemaphoreTask {
        final long requiredPermits;
        final Supplier<CompletableFuture<?>> task;
        final Executor executor;

        public AsyncSemaphoreTask(long requiredPermits, Supplier<CompletableFuture<?>> task, Executor executor) {
            this.requiredPermits = requiredPermits;
            this.task = task;
            this.executor = executor;
        }
    }
}
