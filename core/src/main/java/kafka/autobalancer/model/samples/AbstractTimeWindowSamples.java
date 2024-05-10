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

import kafka.autobalancer.model.AbstractInstanceUpdater;

import java.util.Deque;
import java.util.LinkedList;

public abstract class AbstractTimeWindowSamples {
    protected final Deque<Window> windows;
    private final int validWindowSize;
    private final int maxWindowSize;
    private final int windowCapacity;

    public AbstractTimeWindowSamples(int validWindowSize, int maxWindowSize, int windowCapacity) {
        if (validWindowSize <= 0 || validWindowSize > maxWindowSize || windowCapacity <= 0) {
            throw new IllegalArgumentException("Invalid window size");
        }
        this.windows = new LinkedList<>();
        this.validWindowSize = validWindowSize;
        this.maxWindowSize = maxWindowSize;
        this.windowCapacity = windowCapacity;
    }

    public void append(double value) {
        Window window = windows.peekLast();
        if (window == null || !window.append(value)) {
            window = rollNextWindow();
            window.append(value);
        }
        if (windows.size() > maxWindowSize) {
            windows.pop();
        }
    }

    public boolean isTrusted() {
        if (windows.size() < validWindowSize) {
            return false;
        }
        return isTrustedWindowData();
    }

    protected abstract boolean isTrustedWindowData();

    public AbstractInstanceUpdater.Load ofLoad() {
        return new AbstractInstanceUpdater.Load(isTrusted(), getLatest());
    }

    private Window rollNextWindow() {
        Window window = new Window(windowCapacity);
        windows.offer(window);
        return window;
    }

    public double getLatest() {
        Window window = windows.peekLast();
        if (window == null) {
            return 0;
        }
        return window.latest();
    }

    public int size() {
        return windows.size();
    }

    protected static class Window {
        private final double[] values;
        private final int capacity;
        private int size;
        private double sum;
        public Window(int capacity) {
            this.capacity = capacity;
            this.values = new double[capacity];
            this.size = 0;
        }

        public boolean append(double value) {
            if (size == capacity) {
                return false;
            }
            values[size++] = value;
            sum += value;
            return true;
        }

        public int size() {
            return size;
        }

        public double sum() {
            return sum;
        }

        public double get(int index) {
            if (index < 0 || index >= size) {
                throw new IllegalArgumentException(String.format("Invalid index %d, size %d", index, size));
            }
            return values[index];
        }

        public double latest() {
            if (size == 0) {
                return 0;
            }
            return values[size - 1];
        }
    }
}

