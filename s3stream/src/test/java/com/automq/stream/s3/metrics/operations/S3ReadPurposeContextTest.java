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

package com.automq.stream.s3.metrics.operations;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies synchronous scope lifetime, nesting, failure cleanup and thread isolation. */
@Tag("S3Unit")
@Timeout(10)
class S3ReadPurposeContextTest {
    /** Nested scopes restore their caller's purpose even when the nested action throws. */
    @Test
    void restoresNestedScopes() {
        assertEquals(S3ReadPurpose.OTHER, S3ReadPurposeContext.current());
        S3ReadPurposeContext.withPurpose(S3ReadPurpose.ROUTER_PRODUCE, () -> {
            assertEquals(S3ReadPurpose.SNAPSHOT_REPLAY, S3ReadPurposeContext.withPurpose(
                S3ReadPurpose.SNAPSHOT_REPLAY, S3ReadPurposeContext::current));
            assertEquals(S3ReadPurpose.ROUTER_PRODUCE, S3ReadPurposeContext.current());
            IllegalStateException failure = new IllegalStateException("read failed");
            assertSame(failure, assertThrows(IllegalStateException.class, () ->
                S3ReadPurposeContext.withPurpose(S3ReadPurpose.SNAPSHOT_REPLAY, () -> {
                    throw failure;
                })));
            assertEquals(S3ReadPurpose.ROUTER_PRODUCE, S3ReadPurposeContext.current());
            return null;
        });
        assertEquals(S3ReadPurpose.OTHER, S3ReadPurposeContext.current());
    }

    /** Returning an unfinished future ends the scope before any completion callback runs. */
    @Test
    void restoresBeforeFutureCompletion() {
        CompletableFuture<Void> pending = new CompletableFuture<>();
        assertSame(pending, S3ReadPurposeContext.withPurpose(S3ReadPurpose.ROUTER_PRODUCE, () -> pending));
        assertFalse(pending.isDone());
        assertEquals(S3ReadPurpose.OTHER, S3ReadPurposeContext.current());
        CompletableFuture<S3ReadPurpose> callback = pending.thenApply(nil -> S3ReadPurposeContext.current());
        pending.complete(null);
        assertEquals(S3ReadPurpose.OTHER, callback.join());
    }

    /** Another thread sees no implicit context; explicit re-entry also restores that worker's previous value. */
    @Test
    void isolatesThreads() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<S3ReadPurpose> unscoped = S3ReadPurposeContext.withPurpose(S3ReadPurpose.ROUTER_PRODUCE,
                () -> CompletableFuture.supplyAsync(S3ReadPurposeContext::current, executor));
            assertEquals(S3ReadPurpose.OTHER, unscoped.get(5, TimeUnit.SECONDS));
            CompletableFuture<S3ReadPurpose> captured = S3ReadPurposeContext.withPurpose(S3ReadPurpose.ROUTER_PRODUCE, () -> {
                S3ReadPurpose purpose = S3ReadPurposeContext.current();
                return CompletableFuture.supplyAsync(() -> S3ReadPurposeContext.withPurpose(purpose,
                    S3ReadPurposeContext::current), executor);
            });
            assertEquals(S3ReadPurpose.ROUTER_PRODUCE, captured.get(5, TimeUnit.SECONDS));
            assertEquals(S3ReadPurpose.OTHER, executor.submit(S3ReadPurposeContext::current).get(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }
}
