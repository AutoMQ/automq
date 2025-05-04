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
