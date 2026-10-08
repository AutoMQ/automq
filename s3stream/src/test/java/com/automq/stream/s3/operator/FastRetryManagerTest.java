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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
@Timeout(10)
class FastRetryManagerTest {
    private FastRetryTask task(CompletableFuture<Void> retry) {
        FastRetryTask task = mock(FastRetryTask.class);
        when(task.execute()).thenReturn(retry);
        return task;
    }

    /** Given one active retry, the second remains queued and starts after the permit is released. */
    @Test
    void testPermitExhaustionQueuesRetry() {
        try (FastRetryManager manager = new FastRetryManager("test-")) {
            CompletableFuture<Void> retry = new CompletableFuture<>();
            FastRetryTask active = task(retry);
            manager.schedule(active, 1);
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> verify(active).execute());
            FastRetryTask queued = task(CompletableFuture.completedFuture(null));
            manager.schedule(queued, 1);
            // The timer checks the second task before enqueuing it.
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> verify(queued).isDone());
            verify(queued, never()).execute();
            retry.completeExceptionally(new RuntimeException("retry failed"));
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> verify(queued).execute());
            verify(active).execute();
        }
    }

    /** A queued task whose original request completes is discarded instead of executed. */
    @Test
    void testQueuedCompletedRequestIsDiscarded() {
        try (FastRetryManager manager = new FastRetryManager("test-")) {
            CompletableFuture<Void> retry = new CompletableFuture<>();
            FastRetryTask active = task(retry);
            manager.schedule(active, 1);
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> verify(active).execute());
            FastRetryTask queued = task(CompletableFuture.completedFuture(null));
            manager.schedule(queued, 1);
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> verify(queued).isDone());
            when(queued.isDone()).thenReturn(true);
            retry.complete(null);
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> verify(queued).discard());
            verify(queued, never()).execute();
        }
    }

    /** A full bounded queue discards the new task without waiting or executing it. */
    @Test
    void testFullQueueDiscardsTask() {
        try (FastRetryManager manager = new FastRetryManager("test-")) {
            CompletableFuture<Void> activeRetry = new CompletableFuture<>();
            FastRetryTask active = task(activeRetry);
            manager.schedule(active, 1);
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> verify(active).execute());
            for (int i = 0; i < 4096; i++) {
                manager.schedule(task(new CompletableFuture<>()), 1);
            }
            FastRetryTask overflow = task(new CompletableFuture<>());
            manager.schedule(overflow, 1);
            await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> verify(overflow).discard());
            verify(overflow, never()).execute();
            activeRetry.complete(null);
        }
    }

    /** Closing before the timer expires releases the write task's pre-retained buffer exactly once. */
    @Test
    void testCloseDiscardsPendingTimer() {
        ByteBuf data = TestUtils.randomPooled(1024);
        FastRetryWriteTask task = new FastRetryWriteTask(new CompletableFuture<>(), new CompletableFuture<>(),
            () -> CompletableFuture.completedFuture(null), data,
            (isUsefulRetry, apiCostMillis, limiterAwaitTimeMillis) -> { });
        assertEquals(2, data.refCnt());
        try (FastRetryManager manager = new FastRetryManager("test-")) {
            manager.schedule(task, 60000);
        }
        assertEquals(1, data.refCnt());
        data.release();
    }

    /** Only the first successful result is useful; the callback receives the task's timing measurements. */
    @Test
    void testReadResultCallbackAndDuplicateCleanup() throws Exception {
        CompletableFuture<ByteBuf> attempt = new CompletableFuture<>();
        CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        CompletableFuture<ByteBuf> retry = new CompletableFuture<>();
        AtomicBoolean useful = new AtomicBoolean();
        AtomicLong cost = new AtomicLong(-1);
        AtomicLong wait = new AtomicLong(-1);
        FastRetryReadTask task = new FastRetryReadTask(attempt, result, () -> retry,
            (isUsefulRetry, apiCostMillis, limiterAwaitTimeMillis) -> {
                useful.set(isUsefulRetry);
                cost.set(apiCostMillis);
                wait.set(limiterAwaitTimeMillis);
            });
        task.markEnqueued();
        CompletableFuture<Void> execution = task.execute();
        ByteBuf originalBuffer = TestUtils.randomPooled(1024);
        result.complete(originalBuffer);
        ByteBuf duplicate = TestUtils.randomPooled(1024);
        retry.complete(duplicate);
        execution.get(1, TimeUnit.SECONDS);
        assertEquals(false, useful.get());
        assertTrue(cost.get() >= 0);
        assertTrue(wait.get() >= 0);
        assertEquals(0, duplicate.refCnt());
        originalBuffer.release();
    }

    /** A successful speculative write wins the final future and releases its retained reference first. */
    @Test
    void testWriteResultCallbackWinsFinalFuture() throws Exception {
        ByteBuf data = TestUtils.randomPooled(1024);
        CompletableFuture<Void> result = new CompletableFuture<>();
        AtomicBoolean useful = new AtomicBoolean();
        FastRetryWriteTask task = new FastRetryWriteTask(new CompletableFuture<>(), result,
            () -> CompletableFuture.completedFuture(null), data,
            (isUsefulRetry, apiCostMillis, limiterAwaitTimeMillis) -> useful.set(isUsefulRetry));
        CompletableFuture<Void> completionCheck = result.thenRun(() -> assertEquals(1, data.refCnt()));
        task.markEnqueued();
        task.execute().get(1, TimeUnit.SECONDS);
        completionCheck.get(1, TimeUnit.SECONDS);
        assertTrue(useful.get());
        assertTrue(result.isDone());
        assertEquals(1, data.refCnt());
        data.release();
    }

    /** A late successful write cannot win an already failed final future or report itself useful. */
    @Test
    void testWriteResultCallbackDoesNotWinCompletedFinalFuture() throws Exception {
        ByteBuf data = TestUtils.randomPooled(1024);
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture<Void> retry = new CompletableFuture<>();
        AtomicBoolean useful = new AtomicBoolean(true);
        FastRetryWriteTask task = new FastRetryWriteTask(new CompletableFuture<>(), result, () -> retry, data,
            (isUsefulRetry, apiCostMillis, limiterAwaitTimeMillis) -> useful.set(isUsefulRetry));
        task.markEnqueued();
        CompletableFuture<Void> execution = task.execute();
        result.completeExceptionally(new IllegalStateException("original request aborted"));
        retry.complete(null);
        execution.get(1, TimeUnit.SECONDS);
        assertEquals(false, useful.get());
        assertTrue(result.isCompletedExceptionally());
        assertEquals(1, data.refCnt());
        data.release();
    }
}
