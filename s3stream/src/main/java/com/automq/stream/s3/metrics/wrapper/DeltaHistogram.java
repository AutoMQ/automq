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

package com.automq.stream.s3.metrics.wrapper;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;

public class DeltaHistogram {
    private static final Long DEFAULT_SNAPSHOT_INTERVAL_MS = 5000L;
    private final AtomicLong cumulativeCount = new AtomicLong(0L);
    private final AtomicLong cumulativeSum = new AtomicLong(0L);
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
//    private final Sample sample = new ExponentiallyDecayingSample(1028, 0.015D);
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
        cumulativeCount.incrementAndGet();
        cumulativeSum.addAndGet(value);
//        sample.update(value);
        updateMax(value);
        updateMin(value);
    }

    public long cumulativeCount() {
        return cumulativeCount.get();
    }

    public long cumulativeSum() {
        return cumulativeSum.get();
    }

//    public double p99() {
//        snapshotAndReset();
//        return lastSnapshot.snapshot.get99thPercentile();
//    }

//    public double p50() {
//        snapshotAndReset();
//        return lastSnapshot.snapshot.getMedian();
//    }

    private void snapshotAndReset() {
        synchronized (this) {
            if (lastSnapshot == null || System.currentTimeMillis() - lastSnapshotTime > snapshotInterval) {
//                Snapshot snapshot = sample.getSnapshot();
                long snapshotMin = min.get();
                long snapshotMax = max.get();
                long snapshotCount = cumulativeCount.get() - lastCount;
                long snapshotSum = cumulativeSum.get() - lastSum;
                lastCount = cumulativeCount.get();
                lastSum = cumulativeSum.get();
//                sample.clear();
                min.set(0);
                max.set(0);
                lastSnapshot = new SnapshotExt(snapshotMin, snapshotMax, snapshotCount, snapshotSum);
                lastSnapshotTime = System.currentTimeMillis();
            }
        }
    }

    static class SnapshotExt {
        final long min;
        final long max;
        final long count;
        final long sum;
//        final Snapshot snapshot;

        public SnapshotExt(long min, long max, long count, long sum) {
            this.min = min;
            this.max = max;
            this.count = count;
            this.sum = sum;
//            this.snapshot = snapshot;
        }

        double mean() {
            if (count == 0) {
                return 0;
            } else {
                return (double) sum / count;
            }
        }
    }
}
