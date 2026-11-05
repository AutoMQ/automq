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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Collects a GET response one SDK buffer at a time. Each received buffer is charged at its actual
 * size before requesting the next buffer, and the final grant must finish before returning the data.
 * This controls subsequent demand, not the bytes already received or prefetched by the SDK.
 * Quota requests use a valid configured strategy and are expected to complete successfully.
 */
final class ThrottledAsyncResponseSubscriber implements Subscriber<ByteBuffer> {
    private final NetworkBandwidthLimiter limiter;
    private final ThrottleStrategy strategy;
    // Avoid consolidation, which would copy accumulated response bytes.
    private final CompositeByteBuf data = Unpooled.compositeBuffer(Integer.MAX_VALUE);
    private final CompletableFuture<ByteBuf> result = new CompletableFuture<>();
    private Subscription subscription;
    private ByteBuffer pending;
    private CompletableFuture<Void> grant;
    private boolean requested;
    private boolean upstreamComplete;
    private boolean terminated;
    private boolean draining;

    ThrottledAsyncResponseSubscriber(NetworkBandwidthLimiter limiter, ThrottleStrategy strategy) {
        this.limiter = limiter;
        this.strategy = strategy;
    }

    CompletableFuture<ByteBuf> result() {
        return result;
    }

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        drain();
    }

    @Override
    public synchronized void onNext(ByteBuffer bytes) {
        if (terminated) {
            return;
        }
        requested = false;
        pending = bytes.slice();
        grant = limiter.consume(strategy, pending.remaining());
        if (!grant.isDone()) {
            grant.whenComplete((nil, ex) -> {
                synchronized (ThrottledAsyncResponseSubscriber.this) {
                    drain();
                }
            });
        }
        drain();
    }

    @Override
    public synchronized void onComplete() {
        upstreamComplete = true;
        drain();
    }

    @Override
    public synchronized void onError(Throwable error) {
        if (!terminated) {
            terminated = true;
            pending = null;
            grant = null;
            data.release();
            result.completeExceptionally(error);
        }
    }

    private void drain() {
        // SDK request(1) may synchronously invoke onNext/onComplete; keep those calls on this loop.
        if (draining || terminated) {
            return;
        }
        draining = true;
        try {
            while (!terminated) {
                if (pending != null) {
                    if (!grant.isDone()) {
                        return;
                    }
                    data.addComponent(true, Unpooled.wrappedBuffer(pending));
                    pending = null;
                    grant = null;
                }
                if (upstreamComplete) {
                    terminated = true;
                    result.complete(data);
                    return;
                }
                if (requested) {
                    return;
                }
                requested = true;
                subscription.request(1);
            }
        } catch (RuntimeException ex) {
            onError(ex);
            subscription.cancel();
        } finally {
            draining = false;
        }
    }
}
