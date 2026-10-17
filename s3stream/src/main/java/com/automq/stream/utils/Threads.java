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

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Threads {
    private static final Logger LOGGER = LoggerFactory.getLogger(Threads.class);

    public static final ScheduledExecutorService COMMON_SCHEDULER = newSingleThreadScheduledExecutor("automq-common-scheduler", true, LOGGER);

    public static ExecutorService newFixedThreadPool(int nThreads, ThreadFactory threadFactory, Logger logger) {
        return createFixedThreadPool(nThreads, threadFactory, Integer.MAX_VALUE, logger);
    }

    /**
     * Creates a fixed-size executor with named daemon or non-daemon threads and an effectively unbounded queue.
     */
    public static ExecutorService newFixedThreadPool(int nThreads, String namePrefix, boolean isDaemon,
        Logger logger) {
        return newFixedThreadPool(nThreads, namePrefix, isDaemon, Integer.MAX_VALUE, logger);
    }

    /**
     * Creates a fixed-size executor with named daemon or non-daemon threads and the supplied queue capacity.
     * Saturated executors apply caller-runs backpressure.
     */
    public static ExecutorService newFixedThreadPool(int nThreads, String namePrefix, boolean isDaemon,
        int queueCapacity, Logger logger) {
        return createFixedThreadPool(nThreads, ThreadUtils.createThreadFactory(namePrefix + "-%d", isDaemon),
            queueCapacity, logger);
    }

    /**
     * Creates a fixed-size executor backed by fast-thread-local threads and an effectively unbounded queue.
     */
    public static ExecutorService newFixedFastThreadLocalThreadPool(int nThreads, String namePrefix,
        boolean isDaemon, Logger logger) {
        return createFixedThreadPool(nThreads,
            ThreadUtils.createFastThreadLocalThreadFactory(namePrefix + "-%d", isDaemon), Integer.MAX_VALUE, logger);
    }

    private static ThreadPoolExecutor createFixedThreadPool(int nThreads, ThreadFactory threadFactory, int queueCapacity,
        Logger logger) {
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(queueCapacity), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy()) {
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                if (t != null) {
                    logger.error("[FATAL] Uncaught exception in executor thread {}", Thread.currentThread().getName(), t);
                }
            }
        };
    }

    /**
     * Creates a bounded cached thread pool whose workers expire after 60 seconds of inactivity.
     */
    public static ThreadPoolExecutor newCachedThreadPool(int maximumPoolSize, String pattern, boolean daemon,
        Logger logger) {
        ThreadPoolExecutor executor = createFixedThreadPool(maximumPoolSize,
            ThreadUtils.createThreadFactory(pattern, daemon), Integer.MAX_VALUE, logger);
        executor.setKeepAliveTime(60L, TimeUnit.SECONDS);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * Creates a virtual-thread-per-task executor on JDK 21 or a bounded cached platform-thread executor on JDK 17.
     * Reflection preserves the Java 17 compile-time and runtime baseline.
     */
    public static ExecutorService newVirtualThreadOrCachedThreadPool(int maximumPoolSize, String pattern,
        boolean daemon, Logger logger) {
        if (Runtime.version().feature() < 21) {
            return newCachedThreadPool(maximumPoolSize, pattern, daemon, logger);
        }
        try {
            Object virtualThreadBuilder = Thread.class.getMethod("ofVirtual").invoke(null);
            Class<?> threadBuilderClass = Class.forName("java.lang.Thread$Builder");
            ThreadFactory virtualThreadFactory = (ThreadFactory) threadBuilderClass.getMethod("factory")
                .invoke(virtualThreadBuilder);
            AtomicLong threadEpoch = new AtomicLong();
            ThreadFactory namedVirtualThreadFactory = runnable -> {
                Thread thread = virtualThreadFactory.newThread(runnable);
                thread.setName(threadName(pattern, threadEpoch.incrementAndGet()));
                return thread;
            };
            return (ExecutorService) Executors.class
                .getMethod("newThreadPerTaskExecutor", ThreadFactory.class)
                .invoke(null, namedVirtualThreadFactory);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            return newCachedThreadPool(maximumPoolSize, pattern, daemon, logger);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Unable to access virtual-thread APIs", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Unable to create virtual-thread executor", e.getCause());
        }
    }

    private static String threadName(String pattern, long epoch) {
        return pattern.contains("%d") ? String.format(pattern, epoch) : pattern;
    }

    /** Creates a single-worker executor using a named daemon or non-daemon thread. */
    public static ThreadPoolExecutor newSingleThreadExecutor(String pattern, boolean daemon, Logger logger) {
        return createFixedThreadPool(1, ThreadUtils.createThreadFactory(pattern, daemon), Integer.MAX_VALUE, logger);
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
