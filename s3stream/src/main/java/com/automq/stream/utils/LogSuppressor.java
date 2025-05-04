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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LogSuppressor {
    private final Logger logger;
    private final long intervalMills;
    private final AtomicLong lastLogTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicInteger suppressedCount = new AtomicInteger();

    public LogSuppressor(Logger logger, long intervalMills) {
        this.logger = logger;
        this.intervalMills = intervalMills;
    }

    public void error(String message, Object... args) {
        long now = System.currentTimeMillis();
        for (; ; ) {
            long last = lastLogTime.get();
            if (now - last > intervalMills) {
                if (lastLogTime.compareAndSet(last, now)) {
                    logger.error("[SUPPRESSED_TIME=" + suppressedCount.getAndSet(0) + "] " + message, args);
                    break;
                }
            } else {
                suppressedCount.incrementAndGet();
                break;
            }
        }
    }

    public void warn(String message, Object... args) {
        long now = System.currentTimeMillis();
        for (; ; ) {
            long last = lastLogTime.get();
            if (now - last > intervalMills) {
                if (lastLogTime.compareAndSet(last, now)) {
                    logger.warn("[SUPPRESSED_TIME=" + suppressedCount.getAndSet(0) + "] " + message, args);
                    break;
                }
            } else {
                suppressedCount.incrementAndGet();
                break;
            }
        }
    }

    public void info(String message, Object... args) {
        long now = System.currentTimeMillis();
        for (; ; ) {
            long last = lastLogTime.get();
            if (now - last > intervalMills) {
                if (lastLogTime.compareAndSet(last, now)) {
                    logger.info("[SUPPRESSED_TIME=" + suppressedCount.getAndSet(0) + "] " + message, args);
                    break;
                }
            } else {
                suppressedCount.incrementAndGet();
                break;
            }
        }
    }

}
