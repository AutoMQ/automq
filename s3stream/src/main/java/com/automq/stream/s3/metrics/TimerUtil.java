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

package com.automq.stream.s3.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class TimerUtil {
    private final AtomicLong last = new AtomicLong(System.nanoTime());

    public TimerUtil() {
        reset();
    }

    public void reset() {
        last.set(System.nanoTime());
    }

    public long lastAs(TimeUnit timeUnit) {
        return timeUnit.convert(last.get(), TimeUnit.NANOSECONDS);
    }

    public long elapsedAs(TimeUnit timeUnit) {
        return timeUnit.convert(System.nanoTime() - last.get(), TimeUnit.NANOSECONDS);
    }

    public long elapsedAndResetAs(TimeUnit timeUnit) {
        long now = System.nanoTime();
        return timeUnit.convert(now - last.getAndSet(now), TimeUnit.NANOSECONDS);
    }

    public static long timeElapsedSince(long statNanoTime, TimeUnit timeUnit) {
        return timeUnit.convert(System.nanoTime() - statNanoTime, TimeUnit.NANOSECONDS);
    }

}
