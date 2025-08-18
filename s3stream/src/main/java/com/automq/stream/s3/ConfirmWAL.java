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

package com.automq.stream.s3;

import com.automq.stream.s3.S3Storage.LazyCommit;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.WriteAheadLog;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class ConfirmWAL {
    private final WriteAheadLog log;
    private final Function<LazyCommit, CompletableFuture<Void>> commitHandle;

    public ConfirmWAL(WriteAheadLog log, Function<LazyCommit, CompletableFuture<Void>> commitHandle) {
        this.log = log;
        this.commitHandle = commitHandle;
    }

    public RecordOffset confirmOffset() {
        return log.confirmOffset();
    }

    /**
     * Commit with lazy timeout.
     * If in [0, lazyLingerMs), there is no other commit happened, then trigger a new commit.
     * @param lazyLingerMs lazy linger milliseconds.
     */
    public CompletableFuture<Void> commit(long lazyLingerMs, boolean awaitTrim) {
        return commitHandle.apply(new LazyCommit(lazyLingerMs, awaitTrim));
    }

    public CompletableFuture<Void> commit(long lazyLingerMs) {
        return commit(lazyLingerMs, true);
    }

}
