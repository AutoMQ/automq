/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.utils.threads.EventLoop;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AsyncSemaphore {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSemaphore.class);
    private final ConcurrentLinkedQueue<AsyncSemaphoreTask> tasks = new ConcurrentLinkedQueue<>();
    private final AtomicLong permits;

    public AsyncSemaphore(long permits) {
        this.permits = new AtomicLong(permits);
    }

    /**
     * Acquire permits, if permits are not enough, the task will be added to the queue
     *
     * @param requiredPermits the required permits
     * @param task            task to run when the permits are available, the task should return a CompletableFuture
     *                        which will be completed when the permits could be released.
     * @param eventLoop       the eventLoop to run the task when the permits are available
     * @return true if the permits are acquired, false if the task is added to the waiting queue.
     */
    public boolean acquire(long requiredPermits, Supplier<CompletableFuture<?>> task,
                           EventLoop eventLoop) {
        for (; ; ) {
            long current = permits.get();
            if (current <= 0) {
                tasks.add(new AsyncSemaphoreTask(requiredPermits, task, eventLoop));
                return false;
            }

            if (permits.compareAndSet(current, current - requiredPermits)) {
                try {
                    task.get().whenComplete((nil, ex) -> release(requiredPermits));
                } catch (Throwable e) {
                    LOGGER.error("Error in task", e);
                    release(requiredPermits); // Release permits on error
                }
                return true;
            }
        }
    }

    public boolean requiredRelease() {
        return permits.get() <= 0 || !tasks.isEmpty();
    }

    public long permits() {
        return permits.get();
    }

    void release(long requiredPermits) {
        if (permits.addAndGet(requiredPermits) > 0) {
            AsyncSemaphoreTask t = tasks.poll();
            if (t != null) {
                // use eventLoop to reset the thread stack to avoid stack overflow
                t.eventLoop.execute(() -> acquire(t.requiredPermits, t.task, t.eventLoop));
            }
        }
    }

    static class AsyncSemaphoreTask {
        final long requiredPermits;
        final Supplier<CompletableFuture<?>> task;
        final EventLoop eventLoop;

        public AsyncSemaphoreTask(long requiredPermits, Supplier<CompletableFuture<?>> task, EventLoop eventLoop) {
            this.requiredPermits = requiredPermits;
            this.task = task;
            this.eventLoop = eventLoop;
        }
    }

    @VisibleForTesting
    public int pendingTaskNumber() {
        return tasks.size();
    }
}
