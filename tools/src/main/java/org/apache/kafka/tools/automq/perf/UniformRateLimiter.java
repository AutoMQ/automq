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

package org.apache.kafka.tools.automq.perf;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;

public class UniformRateLimiter {

    private static final AtomicLongFieldUpdater<UniformRateLimiter> V_TIME_UPDATER =
        AtomicLongFieldUpdater.newUpdater(UniformRateLimiter.class, "virtualTime");
    private static final AtomicLongFieldUpdater<UniformRateLimiter> START_UPDATER =
        AtomicLongFieldUpdater.newUpdater(UniformRateLimiter.class, "start");
    private static final double ONE_SEC_IN_NS = SECONDS.toNanos(1);
    private static final long MAX_V_TIME_BACK_SHIFT_SEC = 10;

    private final long intervalNs;
    private final Supplier<Long> nanoClock;

    private volatile long start = Long.MIN_VALUE;
    private volatile long virtualTime;

    public UniformRateLimiter(final double opsPerSec) {
        this(opsPerSec, System::nanoTime);
    }

    private UniformRateLimiter(final double opsPerSec, Supplier<Long> nanoClock) {
        if (Double.isNaN(opsPerSec) || Double.isInfinite(opsPerSec)) {
            throw new IllegalArgumentException("opsPerSec cannot be Nan or Infinite");
        }
        if (opsPerSec <= 0) {
            throw new IllegalArgumentException("opsPerSec must be greater then 0");
        }
        intervalNs = Math.round(ONE_SEC_IN_NS / opsPerSec);
        this.nanoClock = nanoClock;
    }

    public long acquire() {
        final long currOpIndex = V_TIME_UPDATER.getAndIncrement(this);
        long start = this.start;
        if (start == Long.MIN_VALUE) {
            start = nanoClock.get();
            if (!START_UPDATER.compareAndSet(this, Long.MIN_VALUE, start)) {
                start = this.start;
                assert start != Long.MIN_VALUE;
            }
        }
        long intendedTime = start + currOpIndex * intervalNs;
        long now = nanoClock.get();
        // If we are behind schedule too much, update V_TIME
        if (now > intendedTime + MAX_V_TIME_BACK_SHIFT_SEC * ONE_SEC_IN_NS) {
            long newVTime = (now - start) / intervalNs;
            // no need to CAS, updated by multiple threads is acceptable
            V_TIME_UPDATER.set(this, newVTime + 1);
            intendedTime = start + newVTime * intervalNs;
        }
        return intendedTime;
    }

    public static void uninterruptibleSleepNs(final long intendedTime) {
        long sleepNs;
        while ((sleepNs = intendedTime - System.nanoTime()) > 0) {
            LockSupport.parkNanos(sleepNs);
        }
    }
}
