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

package com.automq.stream.s3.metrics.wrapper;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiPredicate;

public class DeltaHistogram {
    private static final Long DEFAULT_SNAPSHOT_INTERVAL_MS = 5000L;
    private final LongAdder cumulativeCount = new LongAdder();
    private final LongAdder cumulativeSum = new LongAdder();
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);

    private final Recorder recorder;
    // the snapshot of histogram for recorder.
    private Histogram intervalHistogram;

    private volatile long snapshotInterval;
    private SnapshotExt lastSnapshot;
    private long lastCount;
    private long lastSum;
    private long lastSnapshotTime;

    public DeltaHistogram() {
        this(DEFAULT_SNAPSHOT_INTERVAL_MS);
    }

    public DeltaHistogram(long snapshotInterval) {
        this.snapshotInterval = snapshotInterval;
        this.recorder = new Recorder(2);
    }

    public void setSnapshotInterval(long snapshotInterval) {
        this.snapshotInterval = snapshotInterval;
    }

    long getSnapshotInterval() {
        return snapshotInterval;
    }

    public long min() {
        snapshotAndReset();
        return lastSnapshot.min;
    }

    public long max() {
        snapshotAndReset();
        return lastSnapshot.max;
    }

    public double mean() {
        snapshotAndReset();
        return lastSnapshot.mean();
    }

    public long count() {
        snapshotAndReset();
        return lastSnapshot.count;
    }

    public long sum() {
        snapshotAndReset();
        return lastSnapshot.sum;
    }

    private void updateMax(long value) {
        update(value, max, (curr, candidate) -> candidate > curr);
    }

    private void updateMin(long value) {
        update(value, min, (curr, candidate) -> candidate < curr);
    }

    private void update(long candidate, AtomicLong target, BiPredicate<Long, Long> predicate) {
        long curr;
        do {
            curr = target.get();
        }
        while (predicate.test(curr, candidate) && !target.compareAndSet(curr, candidate));
    }

    public void record(long value) {
        cumulativeCount.increment();
        cumulativeSum.add(value);
        this.recorder.recordValue(value);
        updateMax(value);
        updateMin(value);
    }

    public long cumulativeCount() {
        return cumulativeCount.sum();
    }

    public long cumulativeSum() {
        return cumulativeSum.sum();
    }

    public double p99() {
        snapshotAndReset();
        return lastSnapshot.p99;
    }

    public double p95() {
        snapshotAndReset();
        return lastSnapshot.p95;
    }

    public double p50() {
        snapshotAndReset();
        return lastSnapshot.p50;
    }

    public SnapshotExt snapshotAndReset() {
        synchronized (this) {
            if (lastSnapshot == null || System.currentTimeMillis() - lastSnapshotTime > snapshotInterval) {
                this.intervalHistogram = this.recorder.getIntervalHistogram(this.intervalHistogram);

                long snapshotMin = min.get();
                long snapshotMax = max.get();

                long newCount = cumulativeCount.sum();
                long newSum = cumulativeSum.sum();

                long snapshotCount = newCount - lastCount;
                long snapshotSum = newSum - lastSum;

                double p99 = intervalHistogram.getValueAtPercentile(99);
                double p95 = intervalHistogram.getValueAtPercentile(95);
                double p50 = intervalHistogram.getValueAtPercentile(50);

                lastCount = newCount;
                lastSum = newSum;

                min.set(0);
                max.set(0);
                lastSnapshot = new SnapshotExt(snapshotMin, snapshotMax, snapshotCount, snapshotSum, p99, p95, p50);
                lastSnapshotTime = System.currentTimeMillis();
            }
        }

        return lastSnapshot;
    }

    public static class SnapshotExt {
        final long min;
        final long max;
        final long count;
        final long sum;
        final double p99;
        final double p95;
        final double p50;

        public SnapshotExt(long min, long max, long count, long sum, double p99, double p95, double p50) {
            this.min = min;
            this.max = max;
            this.count = count;
            this.sum = sum;
            this.p99 = p99;
            this.p50 = p50;
            this.p95 = p95;
        }

        double mean() {
            if (count == 0) {
                return 0;
            } else {
                return (double) sum / count;
            }
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        public long getCount() {
            return count;
        }

        public long getSum() {
            return sum;
        }

        public double getP99() {
            return p99;
        }

        public double getP95() {
            return p95;
        }

        public double getP50() {
            return p50;
        }
    }
}
