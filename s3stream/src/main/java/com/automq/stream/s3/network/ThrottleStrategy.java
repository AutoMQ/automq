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

package com.automq.stream.s3.network;

public enum ThrottleStrategy {
    BYPASS(0, "bypass"),
    CATCH_UP(1, "catchup"),
    COMPACTION(2, "compaction");

    private final int priority;
    private final String name;

    ThrottleStrategy(int priority, String name) {
        this.priority = priority;
        this.name = name;
    }

    public int priority() {
        return priority;
    }

    public String getName() {
        return name;
    }
}
