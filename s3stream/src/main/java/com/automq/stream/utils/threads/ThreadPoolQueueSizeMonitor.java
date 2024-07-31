/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.utils.threads;

import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPoolQueueSizeMonitor implements ThreadPoolStatusMonitor {

    private final int maxQueueCapacity;

    public ThreadPoolQueueSizeMonitor(int maxQueueCapacity) {
        this.maxQueueCapacity = maxQueueCapacity;
    }

    @Override
    public String describe() {
        return "queueSize";
    }

    @Override
    public double value(ThreadPoolExecutor executor) {
        return executor.getQueue().size();
    }

    @Override
    public boolean needPrintJstack(ThreadPoolExecutor executor, double value) {
        return value > maxQueueCapacity * 0.85;
    }
}
