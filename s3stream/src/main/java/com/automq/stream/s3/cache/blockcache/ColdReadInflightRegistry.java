/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.PendingRequestTracker;

import java.util.concurrent.CompletableFuture;

/**
 * Tracks in-flight BlockCache cold reads across the broker process for availability signal collection.
 */
public final class ColdReadInflightRegistry {
    private static final PendingRequestTracker TRACKER = new PendingRequestTracker();

    private ColdReadInflightRegistry() {
    }

    /**
     * Records a cold read future with its start time in {@link System#nanoTime()} units.
     */
    public static void track(CompletableFuture<?> future, long startTimeNanos) {
        TRACKER.track(future, startTimeNanos);
    }

    /**
     * Returns true if any in-flight cold read has been pending for at least {@code thresholdNanos}.
     */
    public static boolean hasPendingOlderThan(long thresholdNanos) {
        return TRACKER.hasPendingOlderThan(thresholdNanos);
    }

    /**
     * Clears process-local state for tests and broker lifecycle cleanup.
     */
    public static void clear() {
        TRACKER.clear();
    }
}
