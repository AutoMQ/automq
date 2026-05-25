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

package com.automq.stream.s3.metrics.stats;

import java.util.Objects;
import java.util.function.LongSupplier;

public class MinIntervalRate {
    private final long minIntervalMs;
    private final LongSupplier nowMsSupplier;
    private long lastUpdateMs;
    private long lastCumulativeValue = 0;
    private double rate = 0.0;

    public MinIntervalRate(long minIntervalMs) {
        this(minIntervalMs, System::currentTimeMillis);
    }

    public MinIntervalRate(long minIntervalMs, LongSupplier nowMsSupplier) {
        if (minIntervalMs <= 0) {
            throw new IllegalArgumentException("minIntervalMs must be positive");
        }
        this.minIntervalMs = minIntervalMs;
        this.nowMsSupplier = Objects.requireNonNull(nowMsSupplier, "nowMsSupplier");
        this.lastUpdateMs = nowMsSupplier.getAsLong();
    }

    public synchronized double update(long cumulativeValue) {
        long nowMs = nowMsSupplier.getAsLong();
        long elapsedMs = nowMs - lastUpdateMs;
        if (elapsedMs < minIntervalMs) {
            return rate;
        }
        if (cumulativeValue < lastCumulativeValue) {
            lastUpdateMs = nowMs;
            lastCumulativeValue = cumulativeValue;
            rate = 0.0;
            return rate;
        }
        rate = (cumulativeValue - lastCumulativeValue) * 1000.0 / elapsedMs;
        lastUpdateMs = nowMs;
        lastCumulativeValue = cumulativeValue;
        return rate;
    }
}
