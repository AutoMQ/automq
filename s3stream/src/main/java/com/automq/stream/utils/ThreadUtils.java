/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.utils;

import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

/**
 * Utilities for working with threads.
 */
public class ThreadUtils {
    /**
     * Create a new ThreadFactory.
     *
     * @param pattern The pattern to use.  If this contains %d, it will be
     *                replaced with a thread number.  It should not contain more
     *                than one %d.
     * @param daemon  True if we want daemon threads.
     * @return The new ThreadFactory.
     */
    public static ThreadFactory createThreadFactory(final String pattern,
        final boolean daemon) {
        return new ThreadFactory() {
            private final AtomicLong threadEpoch = new AtomicLong(0);

            @Override
            public Thread newThread(Runnable r) {
                String threadName;
                if (pattern.contains("%d")) {
                    threadName = String.format(pattern, threadEpoch.addAndGet(1));
                } else {
                    threadName = pattern;
                }
                Thread thread = new Thread(r, threadName);
                thread.setDaemon(daemon);
                return thread;
            }
        };
    }

    public static ThreadFactory createFastThreadLocalThreadFactory(String pattern, final boolean daemon) {
        return new ThreadFactory() {
            private final AtomicLong threadEpoch = new AtomicLong(0);

            @Override
            public Thread newThread(Runnable r) {
                String threadName;
                if (pattern.contains("%d")) {
                    threadName = String.format(pattern, threadEpoch.addAndGet(1));
                } else {
                    threadName = pattern;
                }
                FastThreadLocalThread thread = new FastThreadLocalThread(r, threadName);
                thread.setDaemon(daemon);
                return thread;
            }
        };
    }

    public static Runnable wrapRunnable(Runnable runnable, Logger logger) {
        return () -> {
            try {
                runnable.run();
            } catch (Throwable throwable) {
                logger.error("[FATAL] Uncaught exception in executor thread {}", Thread.currentThread().getName(), throwable);
            }
        };
    }

    /**
     * A wrapper of {@link #shutdownExecutor} without logging.
     */
    public static void shutdownExecutor(ExecutorService executorService, long timeout, TimeUnit timeUnit) {
        shutdownExecutor(executorService, timeout, timeUnit, NOPLogger.NOP_LOGGER);
    }

    /**
     * Shuts down an executor service in two phases, first by calling shutdown to reject incoming tasks,
     * and then calling shutdownNow, if necessary, to cancel any lingering tasks.
     * After the timeout/on interrupt, the service is forcefully closed.
     */
    public static void shutdownExecutor(ExecutorService executorService, long timeout, TimeUnit timeUnit,
        Logger logger) {
        if (null == executorService) {
            return;
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(timeout, timeUnit)) {
                executorService.shutdownNow();
                logger.error("Executor {} did not terminate in time, forcefully shutting down", executorService);
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
