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
