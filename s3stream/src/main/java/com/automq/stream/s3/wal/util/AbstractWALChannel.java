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

package com.automq.stream.s3.wal.util;

import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;

public abstract class AbstractWALChannel implements WALChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractWALChannel.class);

    /**
     * Flag to indicate if the WAL has failed.
     * It will be set to true if an IO operation fails continuously, and it will never be reset.
     * Any IO operation will fail immediately if this flag is true.
     */
    private volatile boolean failed = false;

    @Override
    public void markFailed() {
        this.failed = true;
    }

    @Override
    public void write(ByteBuf src, long position) throws IOException {
        checkFailed();
        doWrite(src, position);
    }

    @Override
    public void retryWrite(ByteBuf src, long position, long retryIntervalMillis,
        long retryTimeoutMillis) throws IOException {
        checkFailed();
        retry(() -> write(src, position), retryIntervalMillis, retryTimeoutMillis);
    }

    @Override
    public void flush() throws IOException {
        checkFailed();
        doFlush();
    }

    @Override
    public void retryFlush(long retryIntervalMillis, long retryTimeoutMillis) throws IOException {
        checkFailed();
        retry(this::flush, retryIntervalMillis, retryTimeoutMillis);
    }

    @Override
    public int read(ByteBuf dst, long position, int length) throws IOException {
        checkFailed();
        return doRead(dst, position, length);
    }

    @Override
    public int retryRead(ByteBuf dst, long position, int length, long retryIntervalMillis,
        long retryTimeoutMillis) throws IOException {
        checkFailed();
        return retry(() -> read(dst, position, length), retryIntervalMillis, retryTimeoutMillis);
    }

    private void retry(IORunnable runnable, long retryIntervalMillis, long retryTimeoutMillis) throws IOException {
        retry(IOSupplier.from(runnable), retryIntervalMillis, retryTimeoutMillis);
    }

    private <T> T retry(IOSupplier<T> supplier, long retryIntervalMillis, long retryTimeoutMillis) throws IOException {
        long start = System.nanoTime();
        long retryTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(retryTimeoutMillis);
        while (true) {
            try {
                return supplier.get();
            } catch (IOException e) {
                if (System.nanoTime() - start > retryTimeoutNanos) {
                    failed = true;
                    LOGGER.error("Failed to execute IO operation, retry timeout", e);
                    throw e;
                }
                checkFailed();
                LOGGER.warn("Failed to execute IO operation, retrying in {}ms, error: {}", retryIntervalMillis, e.getMessage());
                Threads.sleep(retryIntervalMillis);
            }
        }
    }

    private void checkFailed() throws IOException {
        if (failed) {
            IOException e = new IOException("Failed to execute IO operation, WAL failed");
            LOGGER.error("Failed to execute IO operation, WAL failed", e);
            throw e;
        }
    }

    protected abstract void doWrite(ByteBuf src, long position) throws IOException;

    protected abstract void doFlush() throws IOException;

    protected abstract int doRead(ByteBuf dst, long position, int length) throws IOException;

    private interface IOSupplier<T> {
        T get() throws IOException;

        static IOSupplier<Void> from(IORunnable runnable) {
            return () -> {
                runnable.run();
                return null;
            };
        }
    }

    private interface IORunnable {
        void run() throws IOException;
    }
}
