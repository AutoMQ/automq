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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.operator.ReadProgressListener;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Owns one block-sized prefix buffer for a cache fill. Only one attempt contributes bytes at a time;
 * failed attempts discard their prefix, and competing attempts never append to each other's data.
 * Pending callers receive independent copies only after validating a complete record. Normal fill
 * completion owns the fallback futures. The cache keeps the block reserved until the normal range-read
 * future completes, even if every caller has already returned. SDK response buffers are additional storage
 * overhead; this prefix replaces, rather than supplements, the completed cache buffer.
 */
final class RouterWALReadProgress implements AutoCloseable {
    private final int blockSize;
    private final Runnable onEarlyRead;
    private final List<PendingRead> waiting = new ArrayList<>();
    private ByteBuf prefix;
    private Attempt active;
    private boolean closed;

    RouterWALReadProgress(int blockSize, Runnable onEarlyRead) {
        this.blockSize = blockSize;
        this.onEarlyRead = onEarlyRead;
    }

    void add(int start, int end, CompletableFuture<ByteBuf> result, Predicate<ByteBuf> validator) {
        publishIfReady(new PendingRead(start, end, result, validator));
    }

    private void publishIfReady(PendingRead read) {
        ByteBuf copy;
        synchronized (this) {
            if (closed || read.result.isDone()) {
                return;
            }
            if (prefix == null || read.end > prefix.readableBytes()) {
                waiting.add(read);
                return;
            }
            copy = prefix.copy(read.start, read.end - read.start);
        }
        publish(read, copy);
    }

    ReadProgressListener newAttempt() {
        return new Attempt();
    }

    private void publish(PendingRead read, ByteBuf copy) {
        boolean transferred = false;
        try {
            // Validation cannot weaken the ordinary completed-download path or poison other callers.
            if (!read.result.isDone() && read.validator.test(copy.asReadOnly())) {
                transferred = read.result.complete(copy);
                if (transferred) {
                    onEarlyRead.run();
                }
            }
        } catch (RuntimeException ignored) {
            // An unvalidated range keeps waiting for the original full-response future.
        } finally {
            if (!transferred) {
                copy.release();
            }
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        waiting.clear();
        clearPrefix();
    }

    private void clearPrefix() {
        if (prefix != null) {
            prefix.release();
            prefix = null;
        }
        active = null;
    }

    private final class Attempt implements ReadProgressListener {
        private boolean failed;

        @Override
        public void onProgress(ByteBuf data) {
            List<PendingRead> ready = new ArrayList<>();
            synchronized (RouterWALReadProgress.this) {
                if (closed || failed || (active != null && active != this)) {
                    return;
                }
                int size = data.readableBytes();
                if (size > blockSize || (prefix != null && size < prefix.readableBytes())) {
                    throw new IllegalArgumentException("Invalid RouterWAL download prefix size: " + size);
                }
                if (prefix == null) {
                    prefix = Unpooled.buffer(blockSize, blockSize);
                    active = this;
                }
                int received = prefix.readableBytes();
                prefix.writeBytes(data, data.readerIndex() + received, size - received);
                Iterator<PendingRead> it = waiting.iterator();
                while (it.hasNext()) {
                    PendingRead read = it.next();
                    if (read.result.isDone()) {
                        it.remove();
                    } else if (read.end <= size) {
                        ready.add(read);
                        it.remove();
                    }
                }
            }
            // Neither validation nor completion callbacks run while holding the prefix lock.
            ready.forEach(RouterWALReadProgress.this::publishIfReady);
        }

        @Override
        public void onFailure() {
            synchronized (RouterWALReadProgress.this) {
                failed = true;
                if (active == this) {
                    clearPrefix();
                }
            }
        }
    }

    private record PendingRead(int start, int end, CompletableFuture<ByteBuf> result, Predicate<ByteBuf> validator) {
    }
}
