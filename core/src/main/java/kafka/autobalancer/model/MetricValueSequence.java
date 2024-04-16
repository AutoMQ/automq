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

package kafka.autobalancer.model;

import java.util.Deque;
import java.util.LinkedList;

public class MetricValueSequence {
    private static final int DEFAULT_MAX_SIZE = 1024;
    private static final int MIN_VALID_LENGTH = 30;
    private final Deque<Double> values;
    private final int maxSize;
    private Snapshot prev;

    public MetricValueSequence() {
        this(DEFAULT_MAX_SIZE);
    }

    public MetricValueSequence(int maxSize) {
        this.maxSize = maxSize;
        this.values = new LinkedList<>();
    }

    public MetricValueSequence copy() {
        MetricValueSequence copy = new MetricValueSequence(maxSize);
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
        if (values.size() < MIN_VALID_LENGTH) {
            return null;
        }
        Snapshot snapshot = new Snapshot(prev, values);
        this.prev = snapshot;
        return snapshot;
    }

    public int size() {
        return values.size();
    }
}
