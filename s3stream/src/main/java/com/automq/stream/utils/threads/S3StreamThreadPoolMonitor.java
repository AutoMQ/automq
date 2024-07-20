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

package com.automq.stream.utils.threads;

import com.automq.stream.utils.ThreadUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3StreamThreadPoolMonitor {
    private static final List<ThreadPoolWrapper> MONITOR_EXECUTOR = new CopyOnWriteArrayList<>();
    private static final ScheduledExecutorService MONITOR_SCHEDULED = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory("ThreadPoolMonitor-%d", true));
    private static Logger waterMarkLogger = LoggerFactory.getLogger(S3StreamThreadPoolMonitor.class);
    private static volatile long threadPoolStatusPeriodTime = TimeUnit.SECONDS.toMillis(3);

    public static void config(Logger waterMarkLoggerConfig, long threadPoolStatusPeriodTimeConfig) {
        waterMarkLogger = waterMarkLoggerConfig;
        threadPoolStatusPeriodTime = threadPoolStatusPeriodTimeConfig;
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        String name,
        boolean isDaemon,
        int queueCapacity) {
        return createAndMonitor(corePoolSize, maximumPoolSize, keepAliveTime, unit, name, isDaemon, queueCapacity, throwable -> null, false);
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
                                                      int maximumPoolSize,
                                                      long keepAliveTime,
                                                      TimeUnit unit,
                                                      String name,
                                                      boolean isDaemon,
                                                      int queueCapacity,
                                                      Function<Throwable, Void> afterExecutionHook,
                                                      boolean fastThreadLocalThread) {
        return createAndMonitor(corePoolSize, maximumPoolSize, keepAliveTime, unit, name, isDaemon, queueCapacity, afterExecutionHook, Collections.emptyList(), fastThreadLocalThread);
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        String name,
        boolean isDaemon,
        int queueCapacity, Function<Throwable, Void> afterExecutionHook,
        ThreadPoolStatusMonitor... threadPoolStatusMonitors) {
        return createAndMonitor(corePoolSize, maximumPoolSize, keepAliveTime, unit, name, isDaemon, queueCapacity, afterExecutionHook,
            List.of(threadPoolStatusMonitors), false);
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        String name,
        boolean isDaemon,
        int queueCapacity,
        Function<Throwable, Void> afterExecutionHook,
        List<ThreadPoolStatusMonitor> threadPoolStatusMonitors,
        boolean fastThreadLocalThread) {

        ThreadFactory threadFactory = fastThreadLocalThread ?
                ThreadUtils.createFastThreadLocalThreadFactory(name + "-%d", isDaemon) :
                ThreadUtils.createThreadFactory(name + "-%d", isDaemon);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            corePoolSize,
            maximumPoolSize,
            keepAliveTime,
            unit,
            new LinkedBlockingQueue<>(queueCapacity),
            threadFactory,
            new ThreadPoolExecutor.DiscardOldestPolicy()) {
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                afterExecutionHook.apply(t);
            }
        };
        List<ThreadPoolStatusMonitor> printers = new ArrayList<>();
        printers.add(new ThreadPoolQueueSizeMonitor(queueCapacity));
        printers.addAll(threadPoolStatusMonitors);

        MONITOR_EXECUTOR.add(ThreadPoolWrapper.builder()
            .name(name)
            .threadPoolExecutor(executor)
            .statusPrinters(printers)
            .build());
        return executor;
    }

    public static void logThreadPoolStatus() {
        for (ThreadPoolWrapper threadPoolWrapper : MONITOR_EXECUTOR) {
            List<ThreadPoolStatusMonitor> monitors = threadPoolWrapper.getStatusPrinters();
            for (ThreadPoolStatusMonitor monitor : monitors) {
                double value = monitor.value(threadPoolWrapper.getThreadPoolExecutor());
                waterMarkLogger.info("\t{}\t{}\t{}", threadPoolWrapper.getName(),
                    monitor.describe(),
                    value);
            }
        }
    }

    public static void init() {
        MONITOR_SCHEDULED.scheduleAtFixedRate(S3StreamThreadPoolMonitor::logThreadPoolStatus, 20,
            threadPoolStatusPeriodTime, TimeUnit.MILLISECONDS);
    }

    public static void shutdown() {
        MONITOR_SCHEDULED.shutdown();
    }
}
