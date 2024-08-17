/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.util;

import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractWALChannel implements WALChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractWALChannel.class);

    @Override
    public void retryWrite(ByteBuf src, long position, long retryIntervalMillis,
        long retryTimeoutMillis) throws IOException {
        retry(() -> write(src, position), retryIntervalMillis, retryTimeoutMillis);
    }

    @Override
    public void retryFlush(long retryIntervalMillis, long retryTimeoutMillis) throws IOException {
        retry(this::flush, retryIntervalMillis, retryTimeoutMillis);
    }

    @Override
    public int retryRead(ByteBuf dst, long position, long retryIntervalMillis,
        long retryTimeoutMillis) throws IOException {
        return retry(() -> read(dst, position), retryIntervalMillis, retryTimeoutMillis);
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
                    LOGGER.error("Failed to execute IO operation, retry timeout", e);
                    throw e;
                } else {
                    LOGGER.warn("Failed to execute IO operation, retrying in {}ms, error: {}", retryIntervalMillis, e.getMessage());
                    Threads.sleep(retryIntervalMillis);
                }
            }
        }
    }

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
