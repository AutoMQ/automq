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

package com.automq.stream.s3;

public enum ByteBufAllocPolicy {

    /**
     * Allocate memory from the heap with pooling.
     */
    POOLED_HEAP(true, false),

    /**
     * Use pooled direct memory.
     */
    POOLED_DIRECT(true, true);

    /**
     * Whether the buffer should be pooled or not.
     */
    private final boolean pooled;

    /**
     * Whether the buffer should be direct or not.
     */
    private final boolean direct;

    ByteBufAllocPolicy(boolean pooled, boolean direct) {
        this.pooled = pooled;
        this.direct = direct;
    }

    public boolean isPooled() {
        return pooled;
    }

    public boolean isDirect() {
        return direct;
    }
}
