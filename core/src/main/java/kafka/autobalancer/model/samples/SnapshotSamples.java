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

package kafka.autobalancer.model.samples;

import kafka.autobalancer.model.Snapshot;

import java.util.Deque;
import java.util.LinkedList;

public class SnapshotSamples {
    private static final int DEFAULT_MAX_SIZE = 1024;
    private final Deque<Double> values;
    private final int maxSize;
    private Snapshot prev;

    public SnapshotSamples() {
        this(DEFAULT_MAX_SIZE);
    }

    public SnapshotSamples(int maxSize) {
        this.maxSize = maxSize;
        this.values = new LinkedList<>();
    }

    public SnapshotSamples copy() {
        SnapshotSamples copy = new SnapshotSamples(maxSize);
        copy.values.addAll(values);
        return copy;
    }

    public void append(double value) {
        while (values.size() == maxSize) {
            values.pop();
        }
        values.offer(value);
    }

    public Snapshot snapshot() {
        Snapshot snapshot = new Snapshot(prev, values);
        this.prev = snapshot;
        return snapshot;
    }

    public int size() {
        return values.size();
    }
}
