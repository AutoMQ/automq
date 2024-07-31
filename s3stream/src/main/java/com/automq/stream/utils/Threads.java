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

import com.automq.stream.utils.threads.S3StreamThreadPoolMonitor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Threads {
    private static final Logger LOGGER = LoggerFactory.getLogger(Threads.class);

    public static final ScheduledExecutorService COMMON_SCHEDULER = newSingleThreadScheduledExecutor("automq-common-scheduler", true, LOGGER);

    public static ExecutorService newFixedThreadPool(int nThreads, ThreadFactory threadFactory, Logger logger) {
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory) {
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                if (t != null) {
                    logger.error("[FATAL] Uncaught exception in executor thread {}", Thread.currentThread().getName(), t);
                }
            }
        };
    }

    public static ExecutorService newFixedFastThreadLocalThreadPoolWithMonitor(int nThreads, String namePrefix, boolean isDaemen,
                                                                               Logger logger) {
        return S3StreamThreadPoolMonitor.createAndMonitor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, namePrefix, isDaemen, Integer.MAX_VALUE, throwable -> {
            if (throwable != null) {
                logger.error("[FATAL] Uncaught exception in executor thread {}", Thread.currentThread().getName(), throwable);
            }
            return null;
        }, true);
    }

    public static ExecutorService newFixedThreadPoolWithMonitor(int nThreads, String namePrefix, boolean isDaemen,
        Logger logger) {
        return S3StreamThreadPoolMonitor.createAndMonitor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, namePrefix, isDaemen, Integer.MAX_VALUE, throwable -> {
            if (throwable != null) {
                logger.error("[FATAL] Uncaught exception in executor thread {}", Thread.currentThread().getName(), throwable);
            }
            return null;
        }, false);
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(String name, boolean daemon,
        Logger logger) {
        return newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory(name, true), logger, false, true);
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(ThreadFactory threadFactory,
        Logger logger) {
        return newSingleThreadScheduledExecutor(threadFactory, logger, false, true);
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(ThreadFactory threadFactory,
        Logger logger, boolean removeOnCancelPolicy) {
        return newSingleThreadScheduledExecutor(threadFactory, logger, removeOnCancelPolicy, true);
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(ThreadFactory threadFactory,
        Logger logger, boolean removeOnCancelPolicy, boolean executeExistingDelayedTasksAfterShutdownPolicy) {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory) {
            @Override
            public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
                command = ThreadUtils.wrapRunnable(command, logger);
                return super.schedule(command, delay, unit);
            }

            @Override
            public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
                TimeUnit unit) {
                command = ThreadUtils.wrapRunnable(command, logger);
                return super.scheduleAtFixedRate(command, initialDelay, period, unit);
            }

            @Override
            public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
                TimeUnit unit) {
                command = ThreadUtils.wrapRunnable(command, logger);
                return super.scheduleWithFixedDelay(command, initialDelay, delay, unit);
            }
        };
        executor.setRemoveOnCancelPolicy(removeOnCancelPolicy);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(executeExistingDelayedTasksAfterShutdownPolicy);
        return executor;
    }

    public static boolean sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // ignore
            return true;
        }
        return false;
    }

}
