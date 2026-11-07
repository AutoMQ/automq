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
    public void append(double value, long timestamp) {
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
        if (this.prev != null) {
            this.prev.setPrev(null);
        }
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
