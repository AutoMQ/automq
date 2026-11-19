/*
 * Copyright 2026, AutoMQ HK Limited.
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

import java.util.List;

/**
 * A single producer's repeating schedule on a monotonic clock. Late dispatch stretches
 * that producer's timeline instead of draining overdue records in an artificial catch-up burst.
 */
final class ReplaySchedule {
    private final List<SampleRecords.Entry> entries;
    private final long cycleNanos;
    private final double speed;
    private long cycleStart;
    private int index;

    ReplaySchedule(List<SampleRecords.Entry> entries, long cycleNanos, double speed, long startNanos) {
        this.entries = entries;
        this.cycleNanos = scale(cycleNanos, speed);
        this.speed = speed;
        this.cycleStart = startNanos;
    }

    static long scale(long nanos, double speed) {
        double scaled = nanos / speed;
        if (!Double.isFinite(speed) || speed <= 0 || nanos < 0 || !Double.isFinite(scaled) || scaled >= Long.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid replay duration or speed");
        }
        return (long) scaled;
    }

    SampleRecords.Entry current() {
        return entries.get(index);
    }

    long deadline() {
        return cycleStart + scale(current().offsetNanos, speed);
    }

    // Returns the amount by which dispatch stretched this producer's schedule.
    long advance(long dispatchedAt) {
        long late = Math.max(0, dispatchedAt - deadline());
        cycleStart += late;
        if (++index == entries.size()) {
            index = 0;
            cycleStart += cycleNanos;
        }
        return late;
    }
}
