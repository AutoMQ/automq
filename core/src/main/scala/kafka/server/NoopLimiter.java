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

package kafka.server;

/**
 * A limiter that does nothing.
 */
public class NoopLimiter implements Limiter {

    public static final NoopLimiter INSTANCE = new NoopLimiter();

    @Override
    public Handler acquire(int permit) throws InterruptedException {
        return new NoopHandler();
    }

    @Override
    public Handler acquire(int permit, long timeoutMs) throws InterruptedException {
        return new NoopHandler();
    }

    @Override
    public int maxPermits() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int availablePermits() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int waitingThreads() {
        return 0;
    }

    @Override
    public String name() {
        return "noop";
    }

    public static class NoopHandler implements Handler {
        @Override
        public void close() {
        }

        @Override
        public void release(int permits) {
        }

        @Override
        public int permitsHeld() {
            return 0;
        }
    }
}
