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

package kafka.autobalancer.model.samples;

import kafka.autobalancer.model.Snapshot;

import java.util.Deque;
import java.util.LinkedList;

public class SnapshottableSamples implements Samples {
    private static final int DEFAULT_MAX_SIZE = 1024;
    private final Deque<Double> values;
    private final int maxSize;
    private Snapshot prev;

    public SnapshottableSamples() {
        this(DEFAULT_MAX_SIZE);
    }

    public SnapshottableSamples(int maxSize) {
        this.maxSize = maxSize;
        this.values = new LinkedList<>();
    }

    public SnapshottableSamples copy() {
        SnapshottableSamples copy = new SnapshottableSamples(maxSize);
        copy.values.addAll(values);
        return copy;
    }

    @Override
    public void append(double value) {
        while (values.size() == maxSize) {
            values.pop();
        }
        values.offer(value);
    }

    @Override
    public double value() {
        if (values.isEmpty()) {
            return 0.0;
        }
        return values.peekLast();
    }

    public Snapshot snapshot() {
        Snapshot snapshot = new Snapshot(prev, values);
        this.prev = snapshot;
        return snapshot;
    }

    public int size() {
        return values.size();
    }

    @Override
    public boolean isTrusted() {
        return false;
    }
}
