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


import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Tag("S3Unit")
@Timeout(10)
class ThrottledAsyncResponseSubscriberTest {
    private final List<Long> sizes = new ArrayList<>();
    private final List<CompletableFuture<Void>> grants = new ArrayList<>();

    private AsyncByteLimiter limiter() {
        return size -> {
            sizes.add((long) size);
            CompletableFuture<Void> grant = new CompletableFuture<>();
            grants.add(grant);
            return grant;
        };
    }

    /** Each SDK buffer is charged at its actual size, and no next demand is issued before its grant. */
    @Test
    void testActualSizeAndSequentialDemand() {
        ThrottledAsyncResponseSubscriber subscriber = new ThrottledAsyncResponseSubscriber(limiter());
        Subscription subscription = mock(Subscription.class);
        subscriber.onSubscribe(subscription);
        verify(subscription).request(1);
        assertTrue(sizes.isEmpty());
        ByteBuffer first = ByteBuffer.wrap(new byte[] {9, 1, 2, 3});
        first.position(1);
        subscriber.onNext(first);
        assertEquals(List.of(3L), sizes);
        verify(subscription).request(1);
        grants.get(0).complete(null);
        verify(subscription, times(2)).request(1);
        // SDK buffers need not match the PUT chunk size, and can be larger than 64 KiB.
        subscriber.onNext(ByteBuffer.allocate(100_000));
        assertEquals(List.of(3L, 100_000L), sizes);
        verify(subscription, times(2)).request(1);
        subscriber.onComplete();
        assertFalse(subscriber.result().isDone());
        grants.get(1).complete(null);
        ByteBuf result = subscriber.result().join();
        try {
            assertEquals(100_003, result.readableBytes());
            assertEquals(1, result.getByte(0));
            assertEquals(3, result.getByte(2));
            assertEquals(1, first.position());
            verify(subscription, times(2)).request(1);
        } finally {
            result.release();
        }
    }

    /** A terminal response error wins over a pending grant, whose later completion must not request more data. */
    @Test
    void testErrorWhileWaitingForGrant() {
        ThrottledAsyncResponseSubscriber subscriber = new ThrottledAsyncResponseSubscriber(limiter());
        Subscription subscription = mock(Subscription.class);
        subscriber.onSubscribe(subscription);
        subscriber.onNext(ByteBuffer.wrap(new byte[] {1}));
        grants.get(0).complete(null);
        subscriber.onNext(ByteBuffer.wrap(new byte[] {2}));
        RuntimeException error = new IllegalStateException("response failed");
        subscriber.onError(error);
        assertSame(error, assertThrows(CompletionException.class, () -> subscriber.result().join()).getCause());
        assertFalse(grants.get(1).isDone());
        grants.get(1).complete(null);
        verify(subscription, times(2)).request(1);
    }

    /** Bytes delivered just before a synchronous stream error are still charged to the limiter. */
    @Test
    void testSynchronousErrorStillChargesReceivedBytes() {
        ThrottledAsyncResponseSubscriber subscriber = new ThrottledAsyncResponseSubscriber(limiter());
        RuntimeException error = new IllegalStateException("response failed");
        subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
                subscriber.onNext(ByteBuffer.wrap(new byte[] {1, 2, 3}));
                subscriber.onError(error);
            }

            @Override
            public void cancel() {
            }
        });
        assertEquals(List.of(3L), sizes);
        assertSame(error, assertThrows(CompletionException.class, () -> subscriber.result().join()).getCause());
        grants.get(0).complete(null);
    }

    /** Synchronous publisher callbacks and immediate grants iterate without recursive request/onNext growth. */
    @Test
    void testSynchronousPublisher() {
        ThrottledAsyncResponseSubscriber subscriber = new ThrottledAsyncResponseSubscriber(bytes -> CompletableFuture.completedFuture(null));
        AtomicInteger emitted = new AtomicInteger();
        subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
                assertEquals(1, n);
                if (emitted.incrementAndGet() <= 10_000) {
                    subscriber.onNext(ByteBuffer.wrap(new byte[] {1}));
                } else {
                    subscriber.onComplete();
                }
            }

            @Override
            public void cancel() {
            }
        });
        ByteBuf result = subscriber.result().join();
        try {
            assertEquals(10_000, result.readableBytes());
        } finally {
            result.release();
        }
    }

    /** An empty response completes without charging any bandwidth. */
    @Test
    void testEmptyResponse() {
        ThrottledAsyncResponseSubscriber subscriber = new ThrottledAsyncResponseSubscriber(limiter());
        subscriber.onSubscribe(mock(Subscription.class));
        subscriber.onComplete();
        ByteBuf result = subscriber.result().join();
        try {
            assertEquals(0, result.readableBytes());
            assertTrue(grants.isEmpty());
        } finally {
            result.release();
        }
    }

    /** Retrying a GET uses a fresh subscriber and charges the received bytes again. */
    @Test
    void testRetryChargesAgain() {
        AsyncByteLimiter limiter = limiter();
        for (int attempt = 0; attempt < 2; attempt++) {
            ThrottledAsyncResponseSubscriber subscriber = new ThrottledAsyncResponseSubscriber(limiter);
            subscriber.onSubscribe(mock(Subscription.class));
            subscriber.onNext(ByteBuffer.wrap(new byte[] {1, 2}));
            subscriber.onComplete();
            assertFalse(subscriber.result().isDone());
            grants.get(attempt).complete(null);
            subscriber.result().join().release();
        }
        assertEquals(List.of(2L, 2L), sizes);
    }
}
