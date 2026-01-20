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

package kafka.automq.zerozone;

import kafka.metrics.KafkaMetricsUtil$;

import org.apache.kafka.common.utils.Time;

import com.yammer.metrics.core.Histogram;

import org.slf4j.Logger;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class RouterPermitLimiter {
    private static final long PERMIT_STAT_INTERVAL_MS = 60_000L;

    private final String logPrefix;
    private final Time time;
    private final Semaphore semaphore;
    private final int maxPermits;
    private final Histogram acquireFailTimeHist;
    private final Logger logger;
    private final AtomicLong lastStatMs = new AtomicLong();

    public RouterPermitLimiter(
        String logPrefix,
        Time time,
        int maxPermits,
        Histogram acquireFailTimeHist,
        Logger logger
    ) {
        this.logPrefix = logPrefix;
        this.time = time;
        this.maxPermits = maxPermits;
        this.semaphore = new Semaphore(maxPermits);
        this.acquireFailTimeHist = acquireFailTimeHist;
        this.logger = logger;
    }

    public int acquire(int permits) {
        int need = Math.min(permits, maxPermits);
        if (need <= 0) {
            return 0;
        }
        long startNanos = time.nanoseconds();
        if (!semaphore.tryAcquire(need)) {
            boolean interrupted = false;
            while (true) {
                try {
                    while (!semaphore.tryAcquire(need, 1, TimeUnit.SECONDS)) {
                        tryPermitStatistics();
                    }
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            acquireFailTimeHist.update(time.nanoseconds() - startNanos);
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return need;
    }

    public int acquireUpTo(int size) {
        int target = Math.min(size, maxPermits);
        if (target <= 0) {
            return 0;
        }
        for (int i = 0; i < 16; i++) {
            int available = semaphore.availablePermits();
            if (available <= 0) {
                return 0;
            }
            int toAcquire = Math.min(target, available);
            if (semaphore.tryAcquire(toAcquire)) {
                return toAcquire;
            }
        }
        return 0;
    }

    public void release(int permits) {
        if (permits > 0) {
            semaphore.release(permits);
        }
    }

    private void tryPermitStatistics() {
        long lastRecordTimestamp = lastStatMs.get();
        long now = time.milliseconds();
        if (now - lastRecordTimestamp > PERMIT_STAT_INTERVAL_MS && lastStatMs.compareAndSet(lastRecordTimestamp, now)) {
            int remainingPermits = semaphore.availablePermits();
            logger.info("{} permit cost, permitAcquireFail={}, remainingPermit={}/{}",
                logPrefix,
                KafkaMetricsUtil$.MODULE$.histToString(acquireFailTimeHist),
                remainingPermits,
                maxPermits
            );
            acquireFailTimeHist.clear();
        }
    }
}
