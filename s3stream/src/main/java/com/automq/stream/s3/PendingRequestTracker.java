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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Tracks pending async requests by their returned future so completion removes the matching start time.
 */
public class PendingRequestTracker {
    private final Map<CompletableFuture<?>, Long> startTimeNanos = new ConcurrentHashMap<>();
    private final LongSupplier nanoTimeSupplier;

    public PendingRequestTracker() {
        this(System::nanoTime);
    }

    public PendingRequestTracker(LongSupplier nanoTimeSupplier) {
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

    public void track(CompletableFuture<?> future, long startTimeNanos) {
        this.startTimeNanos.put(future, startTimeNanos);
        future.whenComplete((ignored, throwable) -> this.startTimeNanos.remove(future));
    }

    public long maxPendingLatencyNanos() {
        return maxPendingLatencyNanos(nanoTimeSupplier.getAsLong());
    }

    public long maxPendingLatencyNanos(long nowNanos) {
        return startTimeNanos.values().stream()
            .map(startTimeNano -> nowNanos - startTimeNano)
            .max(Long::compareTo)
            .orElse(0L);
    }

    public boolean hasPendingOlderThan(long thresholdNanos) {
        long nowNanos = nanoTimeSupplier.getAsLong();
        return startTimeNanos.values().stream().anyMatch(startTimeNano -> nowNanos - startTimeNano >= thresholdNanos);
    }

    public void clear() {
        startTimeNanos.clear();
    }
}
