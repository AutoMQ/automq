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

package kafka.server;

import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * A limiter adapter that admits every acquisition without retaining capacity.
 */
public final class NoopLimiter implements Limiter {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoopLimiter.class);
    private static final Executor EXECUTOR = Threads.newFixedThreadPool(1, "noop-fetch-executor", true, LOGGER);

    public static final NoopLimiter INSTANCE = new NoopLimiter();

    private NoopLimiter() {
    }

    @Override
    public Permit acquire(long permits, AcquireContext context) {
        if (permits < 0) {
            throw new IllegalArgumentException("permits must not be negative");
        }
        if (context == null) {
            throw new IllegalArgumentException("context must not be null");
        }
        return new NoopPermit(permits);
    }

    @Override
    public void execute(String connectionId, Runnable task) {
        EXECUTOR.execute(task);
    }

    private static final class NoopPermit implements Permit {
        private long permitsHeld;

        private NoopPermit(long permitsHeld) {
            this.permitsHeld = permitsHeld;
        }

        @Override
        public void markResponseReady() {
        }

        @Override
        public void close() {
            permitsHeld = 0;
        }

        @Override
        public boolean releaseTo(long newPermits) {
            if (newPermits < 0 || newPermits > permitsHeld) {
                return false;
            }
            permitsHeld = newPermits;
            return true;
        }

        @Override
        public long permitsHeld() {
            return permitsHeld;
        }
    }
}
