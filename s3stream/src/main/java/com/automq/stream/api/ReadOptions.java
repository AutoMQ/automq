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

package com.automq.stream.api;

import com.automq.stream.api.exceptions.FastReadFailFastException;

public class ReadOptions {
    public static final ReadOptions DEFAULT = new ReadOptions();

    private boolean fastRead;
    private boolean pooledBuf;
    private boolean prioritizedRead;

    public static Builder builder() {
        return new Builder();
    }

    public boolean fastRead() {
        return fastRead;
    }

    public boolean pooledBuf() {
        return pooledBuf;
    }

    public boolean prioritizedRead() {
        return prioritizedRead;
    }

    public static class Builder {
        private final ReadOptions options = new ReadOptions();

        /**
         * Read from cache, if the data is not in cache, then fail fast with {@link FastReadFailFastException}.
         */
        public Builder fastRead(boolean fastRead) {
            options.fastRead = fastRead;
            return this;
        }

        /**
         * Use pooled buffer for reading. The caller is responsible for releasing the buffer.
         */
        public Builder pooledBuf(boolean pooledBuf) {
            options.pooledBuf = pooledBuf;
            return this;
        }

        public Builder prioritizedRead(boolean prioritizedRead) {
            options.prioritizedRead = prioritizedRead;
            return this;
        }

        public ReadOptions build() {
            return options;
        }
    }

}
