/*
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

package com.automq.stream.utils.threads;

import com.automq.stream.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class ThreadPoolMonitor {
    private static Logger waterMarkLogger = LoggerFactory.getLogger(ThreadPoolMonitor.class);

    private static final List<ThreadPoolWrapper> MONITOR_EXECUTOR = new CopyOnWriteArrayList<>();
    private static final ScheduledExecutorService MONITOR_SCHEDULED = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("ThreadPoolMonitor-%d", true));

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
        return createAndMonitor(corePoolSize, maximumPoolSize, keepAliveTime, unit, name, isDaemon, queueCapacity, throwable -> null);
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
                                                      int maximumPoolSize,
                                                      long keepAliveTime,
                                                      TimeUnit unit,
                                                      String name,
                                                      boolean isDaemon,
                                                      int queueCapacity,
                                                      Function<Throwable, Void> afterExecutionHook) {
        return createAndMonitor(corePoolSize, maximumPoolSize, keepAliveTime, unit, name, isDaemon, queueCapacity, afterExecutionHook, Collections.emptyList());
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
            List.of(threadPoolStatusMonitors));
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        String name,
        boolean isDaemon,
        int queueCapacity,
        Function<Throwable, Void> afterExecutionHook,
        List<ThreadPoolStatusMonitor> threadPoolStatusMonitors) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            corePoolSize,
            maximumPoolSize,
            keepAliveTime,
            unit,
            new LinkedBlockingQueue<>(queueCapacity),
            ThreadUtils.createThreadFactory(name + "-%d", isDaemon),
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
        MONITOR_SCHEDULED.scheduleAtFixedRate(ThreadPoolMonitor::logThreadPoolStatus, 20,
            threadPoolStatusPeriodTime, TimeUnit.MILLISECONDS);
    }

    public static void shutdown() {
        MONITOR_SCHEDULED.shutdown();
    }
}