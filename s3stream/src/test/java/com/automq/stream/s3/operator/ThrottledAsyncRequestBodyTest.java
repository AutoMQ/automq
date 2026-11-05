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

import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
@Timeout(10)
class ThrottledAsyncRequestBodyTest {
    private final List<Long> sizes = new ArrayList<>();
    private final List<CompletableFuture<Void>> grants = new ArrayList<>();

    private NetworkBandwidthLimiter limiter() {
        NetworkBandwidthLimiter limiter = mock(NetworkBandwidthLimiter.class);
        when(limiter.consume(eq(ThrottleStrategy.WAL_UPLOAD), anyLong())).thenAnswer(invocation -> {
            sizes.add(invocation.getArgument(1));
            CompletableFuture<Void> grant = new CompletableFuture<>();
            grants.add(grant);
            return grant;
        });
        return limiter;
    }

    /** Demand and quota must both exist before a chunk is published, with at most one pending grant. */
    @Test
    void testDemandAndChunkGrants() {
        int chunk = ThrottledAsyncRequestBody.CHUNK_SIZE;
        ByteBuf data = Unpooled.compositeBuffer().addComponents(true,
            Unpooled.buffer(chunk + 5).writeZero(chunk + 5), Unpooled.buffer(chunk + 4).writeZero(chunk + 4));
        data.readerIndex(2);
        data.setByte(data.writerIndex() - 1, 42);
        try (ThrottledAsyncRequestBody body = new ThrottledAsyncRequestBody(data, limiter(), ThrottleStrategy.WAL_UPLOAD)) {
            RecordingSubscriber subscriber = new RecordingSubscriber();
            body.subscribe(subscriber);
            assertEquals(2L * chunk + 7, body.contentLength().orElseThrow());
            assertTrue(grants.isEmpty());
            subscriber.subscription.request(1);
            assertEquals(List.of((long) chunk), sizes);
            assertTrue(subscriber.chunks.isEmpty());
            grants.get(0).complete(null);
            assertEquals(1, subscriber.chunks.size());
            data.setByte(2, 11);
            assertEquals(11, subscriber.chunks.get(0).get(0));
            assertEquals(1, grants.size());
            subscriber.subscription.request(Long.MAX_VALUE);
            assertEquals(2, grants.size());
            grants.get(1).complete(null);
            assertEquals(List.of((long) chunk, 3L, (long) chunk), sizes);
            assertFalse(subscriber.complete);
            grants.get(2).complete(null);
            assertEquals(List.of((long) chunk, 3L, (long) chunk, 4L), sizes);
            grants.get(3).complete(null);
            assertTrue(subscriber.complete);
            assertEquals(List.of(chunk, 3, chunk, 4), subscriber.chunks.stream().map(ByteBuffer::remaining).toList());
            assertEquals(42, subscriber.chunks.get(3).get(3));
            assertEquals(2, data.readerIndex());
        } finally {
            data.release();
        }
    }

    /** A retry subscription starts at the first byte and acquires a new grant for every retransmitted chunk. */
    @Test
    void testRetryReplaysAndPaysAgain() {
        ByteBuf data = Unpooled.wrappedBuffer(new byte[] {1, 2, 3});
        try (ThrottledAsyncRequestBody body = new ThrottledAsyncRequestBody(data, limiter(), ThrottleStrategy.WAL_UPLOAD)) {
            RecordingSubscriber first = new RecordingSubscriber();
            body.subscribe(first);
            first.subscription.request(1);
            grants.get(0).complete(null);
            first.chunks.get(0).get();
            RecordingSubscriber retry = new RecordingSubscriber();
            body.subscribe(retry);
            retry.subscription.request(1);
            assertTrue(retry.chunks.isEmpty());
            grants.get(1).complete(null);
            assertEquals(List.of(3L, 3L), sizes);
            assertEquals(1, retry.chunks.get(0).get());
            assertTrue(retry.complete);
        } finally {
            data.release();
        }
    }

    /** Cancelling a subscription suppresses late grants without cancelling the limiter-owned future. */
    @Test
    void testCancelWhileWaiting() {
        ByteBuf data = Unpooled.buffer(8).writeZero(8);
        try (ThrottledAsyncRequestBody body = new ThrottledAsyncRequestBody(data, limiter(), ThrottleStrategy.WAL_UPLOAD)) {
            RecordingSubscriber subscriber = new RecordingSubscriber();
            body.subscribe(subscriber);
            subscriber.subscription.request(1);
            subscriber.subscription.cancel();
            assertFalse(grants.get(0).isDone());
            grants.get(0).complete(null);
            assertTrue(subscriber.chunks.isEmpty());
            assertFalse(subscriber.complete);
        } finally {
            data.release();
        }
    }

    /** Closing an SDK request releases the retained source and ignores grants that finish afterwards. */
    @Test
    void testCloseWhileWaiting() {
        ByteBuf data = Unpooled.buffer(8).writeZero(8);
        ThrottledAsyncRequestBody body = new ThrottledAsyncRequestBody(data, limiter(), ThrottleStrategy.WAL_UPLOAD);
        data.release();
        RecordingSubscriber subscriber = new RecordingSubscriber();
        body.subscribe(subscriber);
        subscriber.subscription.request(1);
        body.close();
        body.close();
        assertEquals(0, data.refCnt());
        grants.get(0).complete(null);
        assertTrue(subscriber.chunks.isEmpty());
    }

    /** Published chunks share source memory without copying payload bytes. */
    @Test
    void testPublishedChunkSharesSourceMemory() {
        ByteBuf data = Unpooled.wrappedBuffer(new byte[] {7});
        RecordingSubscriber subscriber = new RecordingSubscriber();
        try (ThrottledAsyncRequestBody body = new ThrottledAsyncRequestBody(data, NetworkBandwidthLimiter.NOOP,
            ThrottleStrategy.WAL_UPLOAD)) {
            body.subscribe(subscriber);
            subscriber.subscription.request(1);
            data.setByte(0, 9);
            assertEquals(9, subscriber.chunks.get(0).get());
        } finally {
            data.release();
        }
    }

    /** Synchronous grant completion and demand from onNext use a loop rather than recursive publishing. */
    @Test
    void testSynchronousReentry() {
        ByteBuf data = Unpooled.buffer(ThrottledAsyncRequestBody.CHUNK_SIZE * 128).writeZero(ThrottledAsyncRequestBody.CHUNK_SIZE * 128);
        try (ThrottledAsyncRequestBody body = new ThrottledAsyncRequestBody(data, NetworkBandwidthLimiter.NOOP,
            ThrottleStrategy.WAL_UPLOAD)) {
            RecordingSubscriber subscriber = new RecordingSubscriber() {
                @Override
                public void onNext(ByteBuffer bytes) {
                    super.onNext(bytes);
                    subscription.request(1);
                }
            };
            body.subscribe(subscriber);
            subscriber.subscription.request(1);
            assertEquals(128, subscriber.chunks.size());
            assertTrue(subscriber.complete);
        } finally {
            data.release();
        }
    }

    /** Invalid demand fails, while an empty body completes without consuming quota or needing demand. */
    @Test
    void testInvalidDemandAndEmptyBody() {
        ByteBuf data = Unpooled.buffer(1).writeByte(1);
        try (ThrottledAsyncRequestBody body = new ThrottledAsyncRequestBody(data, limiter(), ThrottleStrategy.WAL_UPLOAD)) {
            RecordingSubscriber subscriber = new RecordingSubscriber();
            body.subscribe(subscriber);
            subscriber.subscription.request(0);
            assertInstanceOf(IllegalArgumentException.class, subscriber.error);
            assertTrue(grants.isEmpty());
        } finally {
            data.release();
        }
        try (ThrottledAsyncRequestBody body = new ThrottledAsyncRequestBody(Unpooled.EMPTY_BUFFER, limiter(),
            ThrottleStrategy.WAL_UPLOAD)) {
            RecordingSubscriber subscriber = new RecordingSubscriber();
            body.subscribe(subscriber);
            assertTrue(subscriber.complete);
            assertTrue(grants.isEmpty());
        }
    }

    private static class RecordingSubscriber implements Subscriber<ByteBuffer> {
        final List<ByteBuffer> chunks = new ArrayList<>();
        Subscription subscription;
        boolean complete;
        Throwable error;

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
        }

        @Override
        public void onNext(ByteBuffer bytes) {
            chunks.add(bytes);
        }

        @Override
        public void onError(Throwable error) {
            this.error = error;
        }

        @Override
        public void onComplete() {
            complete = true;
        }
    }
}
