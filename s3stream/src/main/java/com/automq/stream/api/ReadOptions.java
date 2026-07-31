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

package com.automq.stream.api;

import com.automq.stream.api.exceptions.FastReadFailFastException;

public class ReadOptions {
    public static final ReadOptions DEFAULT = new ReadOptions();

    private final boolean fastRead;
    private final boolean pooledBuf;
    private final boolean prioritizedRead;
    private final boolean snapshotRead;

    public ReadOptions() {
        this(false, false, false, false);
    }

    private ReadOptions(boolean fastRead, boolean pooledBuf, boolean prioritizedRead, boolean snapshotRead) {
        this.fastRead = fastRead;
        this.pooledBuf = pooledBuf;
        this.prioritizedRead = prioritizedRead;
        this.snapshotRead = snapshotRead;
    }

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

    public boolean snapshotRead() {
        return snapshotRead;
    }

    /**
     * Return read options with the requested snapshot-read mode.
     *
     * @deprecated Use {@link #withSnapshotRead(boolean)} to make the copy semantics explicit.
     */
    @Deprecated
    public ReadOptions snapshotRead(boolean snapshotRead) {
        return withSnapshotRead(snapshotRead);
    }

    /**
     * Return read options with the requested snapshot-read mode.
     */
    public ReadOptions withSnapshotRead(boolean snapshotRead) {
        if (this.snapshotRead == snapshotRead) {
            return this;
        }
        return new ReadOptions(fastRead, pooledBuf, prioritizedRead, snapshotRead);
    }

    public static class Builder {
        private boolean fastRead;
        private boolean pooledBuf;
        private boolean prioritizedRead;
        private boolean snapshotRead;

        /**
         * Read from cache, if the data is not in cache, then fail fast with {@link FastReadFailFastException}.
         */
        public Builder fastRead(boolean fastRead) {
            this.fastRead = fastRead;
            return this;
        }

        /**
         * Use pooled buffer for reading. The caller is responsible for releasing the buffer.
         */
        public Builder pooledBuf(boolean pooledBuf) {
            this.pooledBuf = pooledBuf;
            return this;
        }

        public Builder prioritizedRead(boolean prioritizedRead) {
            this.prioritizedRead = prioritizedRead;
            return this;
        }

        public Builder snapshotRead(boolean snapshotRead) {
            this.snapshotRead = snapshotRead;
            return this;
        }

        public ReadOptions build() {
            return new ReadOptions(fastRead, pooledBuf, prioritizedRead, snapshotRead);
        }
    }

}
