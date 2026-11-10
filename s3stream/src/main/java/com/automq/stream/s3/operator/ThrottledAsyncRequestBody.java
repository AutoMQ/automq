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


import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import software.amazon.awssdk.core.async.AsyncRequestBody;

/**
 * Publishes an upload in chunks, acquiring bandwidth only when the SDK requests the next chunk.
 * Each subscription starts at byte zero and pays for its own bytes, including SDK retries.
 * The supplied limiter owns quota policy and any composition of task and network limits.
 * Chunks are shared ByteBuffer views, including for composite sources; payload bytes are not copied.
 * The source is retained until the SDK request finishes and closes this body. The caller must compute
 * a fixed checksum before publishing to detect stale attempts reading released or reused source memory.
 */
final class ThrottledAsyncRequestBody implements AsyncRequestBody, AutoCloseable {
    static final int CHUNK_SIZE = 64 * 1024;
    private final ByteBuf data;
    private final int length;
    private final ByteBuffer[] buffers;
    private final AsyncByteLimiter limiter;
    private final List<AsyncByteLimiter> attempts = new ArrayList<>();
    private boolean subscribed;
    // The body monitor serializes subscriptions, grant callbacks and source release.
    private boolean closed;

    ThrottledAsyncRequestBody(ByteBuf data, AsyncByteLimiter limiter) {
        this.data = data.retainedSlice();
        this.length = data.readableBytes();
        this.buffers = this.data.nioBuffers();
        this.limiter = limiter;
        attempts.add(limiter);
    }

    @Override
    public Optional<Long> contentLength() {
        return Optional.of((long) length);
    }

    @Override
    public synchronized void subscribe(Subscriber<? super ByteBuffer> subscriber) {
        AsyncByteLimiter attempt = limiter;
        if (!closed && subscribed) {
            attempt = limiter.fork();
            attempts.add(attempt);
        }
        subscribed = true;
        UploadSubscription subscription = new UploadSubscription(subscriber, attempt);
        subscriber.onSubscribe(subscription);
        if (closed) {
            subscription.fail(new IllegalStateException("Upload request body is closed"));
        } else {
            subscription.drain();
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            data.release();
            attempts.forEach(AsyncByteLimiter::close);
        }
    }

    private final class UploadSubscription implements Subscription {
        private final Subscriber<? super ByteBuffer> subscriber;
        private final AsyncByteLimiter attemptLimiter;
        private final ByteBuffer[] source = Arrays.stream(buffers).map(ByteBuffer::duplicate).toArray(ByteBuffer[]::new);
        private int bufferIndex;
        private long demand;
        private CompletableFuture<Void> grant;
        private boolean terminated;
        private boolean draining;

        private UploadSubscription(Subscriber<? super ByteBuffer> subscriber, AsyncByteLimiter attemptLimiter) {
            this.subscriber = subscriber;
            this.attemptLimiter = attemptLimiter;
        }

        @Override
        public void request(long n) {
            synchronized (ThrottledAsyncRequestBody.this) {
                if (terminated || closed) {
                    return;
                }
                if (n <= 0) {
                    fail(new IllegalArgumentException("Demand must be positive"));
                    return;
                }
                demand += Math.min(Long.MAX_VALUE - demand, n);
                drain();
            }
        }

        @Override
        public void cancel() {
            synchronized (ThrottledAsyncRequestBody.this) {
                terminated = true;
                // The limiter owns its future; cancellation stops publishing, not quota allocation.
            }
        }

        private void drain() {
            // SDK request() and immediately completed grants may reenter while onNext is running.
            if (draining || terminated || closed) {
                return;
            }
            draining = true;
            CompletableFuture<Void> newGrant = null;
            try {
                while (!terminated && !closed) {
                    while (bufferIndex < source.length && !source[bufferIndex].hasRemaining()) {
                        bufferIndex++;
                    }
                    if (bufferIndex == source.length) {
                        terminated = true;
                        subscriber.onComplete();
                        return;
                    }
                    if (demand == 0) {
                        return;
                    }
                    ByteBuffer buffer = source[bufferIndex];
                    int size = Math.min(CHUNK_SIZE, buffer.remaining());
                    if (grant == null) {
                        grant = attemptLimiter.acquire(size);
                        newGrant = grant;
                    }
                    if (!grant.isDone()) {
                        return;
                    }
                    grant.join();
                    grant = null;
                    ByteBuffer chunk = buffer.slice();
                    chunk.limit(size);
                    buffer.position(buffer.position() + size);
                    demand--;
                    subscriber.onNext(chunk);
                }
            } catch (RuntimeException ex) {
                fail(ex);
            } finally {
                draining = false;
                resumeOnGrant(newGrant);
            }
        }

        private void resumeOnGrant(CompletableFuture<Void> newGrant) {
            if (grant != null && grant == newGrant) {
                grant.whenComplete((nil, ex) -> {
                    synchronized (ThrottledAsyncRequestBody.this) {
                        drain();
                    }
                });
            }
        }

        private void fail(Throwable ex) {
            if (!terminated) {
                terminated = true;
                subscriber.onError(ex);
            }
        }
    }
}
