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

package com.automq.stream.utils;

import com.automq.stream.utils.threads.S3StreamThreadPoolMonitor;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Threads {

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

    public static ExecutorService newFixedThreadPoolWithMonitor(int nThreads, String namePrefix, boolean isDaemen, Logger logger) {
        return S3StreamThreadPoolMonitor.createAndMonitor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, namePrefix, isDaemen, Integer.MAX_VALUE, throwable -> {
            if (throwable != null) {
                logger.error("[FATAL] Uncaught exception in executor thread {}", Thread.currentThread().getName(), throwable);
            }
            return null;
        });
    }

    public static ScheduledThreadPoolExecutor newSingleThreadScheduledExecutor(ThreadFactory threadFactory, Logger logger) {
        return newSingleThreadScheduledExecutor(threadFactory, logger, false);
    }

    public static ScheduledThreadPoolExecutor newSingleThreadScheduledExecutor(ThreadFactory threadFactory, Logger logger, boolean removeOnCancelPolicy) {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory) {
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                if (t != null) {
                    logger.error("[FATAL] Uncaught exception in executor thread {}", Thread.currentThread().getName(), t);
                }
            }
        };
        executor.setRemoveOnCancelPolicy(removeOnCancelPolicy);
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
