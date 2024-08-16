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

package com.automq.stream.s3.index;

import java.util.Random;

public class MockRandom extends Random {
    private long state;

    public MockRandom() {
        this(17);
    }

    public MockRandom(long state) {
        this.state = state;
    }

    @Override
    protected int next(int bits) {
        state = (state * 2862933555777941757L) + 3037000493L;
        return (int) (state >>> (64 - bits));
    }
}
