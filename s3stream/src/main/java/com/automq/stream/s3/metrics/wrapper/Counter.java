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

package com.automq.stream.s3.metrics.wrapper;

import java.util.concurrent.atomic.AtomicLong;

public class Counter {
    private final AtomicLong count = new AtomicLong(0);

    public void inc(long n) {
        count.addAndGet(n);
    }

    public long get() {
        return count.get();
    }
}
