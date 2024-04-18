/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.metrics.wrapper;

import com.yammer.metrics.stats.ExponentiallyDecayingSample;
import com.yammer.metrics.stats.Sample;
import com.yammer.metrics.stats.Snapshot;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;

public class DeltaHistogram {
    private static final Long DEFAULT_SNAPSHOT_INTERVAL_MS = 5000L;
    private final AtomicLong cumulativeCount = new AtomicLong(0L);
    private final AtomicLong cumulativeSum = new AtomicLong(0L);
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
    private final Sample sample = new ExponentiallyDecayingSample(1028, 0.015D);
    private volatile long snapshotInterval;
    private Snapshot lastSnapshot;
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
        return min.get();
    }

    public long max() {
        return max.get();
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
        } while (predicate.test(curr, candidate) && !target.compareAndSet(curr, candidate));
    }

    public void record(long value) {
        cumulativeCount.incrementAndGet();
        cumulativeSum.addAndGet(value);
        sample.update(value);
        updateMax(value);
        updateMin(value);
    }

    public long count() {
        return cumulativeCount.get();
    }

    public long sum() {
        return cumulativeSum.get();
    }

    public double p99() {
        snapshotAndReset();
        return lastSnapshot.get99thPercentile();
    }

    public double p50() {
        snapshotAndReset();
        return lastSnapshot.getMedian();
    }

    private void snapshotAndReset() {
        synchronized (this) {
            if (lastSnapshot == null || System.currentTimeMillis() - lastSnapshotTime > snapshotInterval) {
                lastSnapshot = sample.getSnapshot();
                lastSnapshotTime = System.currentTimeMillis();
                sample.clear();
            }
        }
    }
}
